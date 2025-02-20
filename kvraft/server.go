package kvraft

import (
	"MyRaft/labgob"
	"MyRaft/labrpc"
	"MyRaft/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

type Result struct {
	opType string
	value  string
	err    Err
} // 用于存储processLogEntry操作的传递给通道的值

type Op struct {
	Key         string // 键
	Value       string // 值
	Type        string // 操作类型，Put/Append/Get
	ClientID    int64  // 客户端ID
	SequenceNum int    // 请求序列号
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg // leader->kvserver
	dead    int32              // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data      map[string]string // 存储键值对
	processCh chan Result
}

func (kv *KVServer) processLogEntry() {
	for {
		// 接收Leader需要应用的日志
		rawMsg := <-kv.applyCh
		kv.mu.Lock()
		if rawMsg.CommandValid {
			Msg := rawMsg.Command.(Op)
			kv.mu.Lock()
			result := Result{
				opType: Msg.Type,
				value:  "",
				err:    OK,
			}
			if Msg.Type == "Put" {
				// 如果是Put操作
				kv.data[Msg.Key] = Msg.Value
			} else if Msg.Type == "Append" {
				// 如果是Append操作
				kv.data[Msg.Key] += Msg.Value
			} else if Msg.Type == "Get" {
				// 如果是Get操作，则需要判断是否存在该键
				if value, ok := kv.data[Msg.Key]; ok {
					result.value = value
				} else {
					result.err = ErrNoKey
				}
			}
			kv.processCh <- result
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) WaitApplyCh() Result {
	startTerm, _ := kv.rf.GetState()
	timer := time.NewTimer(1000 * time.Millisecond)
	for {
		select {
		case Msg := <-kv.processCh:
			return Msg
		case <-timer.C:
			curTerm, isLeader := kv.rf.GetState()
			if curTerm != startTerm || !isLeader {
				return Result{
					opType: "",
					value:  "",
					err:    ErrWrongLeader,
				}
			}
			timer.Reset(1000 * time.Millisecond)
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.rf.Start(Op{
		Key:         args.Key,
		Value:       "",
		ClientID:    args.ClientID,
		Type:        "Get",
		SequenceNum: args.SequenceNum,
	})
	kv.mu.Unlock()
	// 等待日志提交
	result := kv.WaitApplyCh()
	reply.Err = result.err
	reply.Value = result.value
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.rf.Start(Op{
		Key:         args.Key,
		Value:       args.Value,
		ClientID:    args.ClientID,
		Type:        args.Op,
		SequenceNum: args.SequenceNum,
	})
	kv.mu.Unlock()
	// 等待日志提交
	result := kv.WaitApplyCh()
	reply.Err = result.err
	kv.mu.Unlock()
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.processCh = make(chan Result)

	return kv
}
