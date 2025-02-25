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
	data      []map[string]string // 存储键值对
	processCh chan Result         // 用于存储processLogEntry操作的传递给通道的值
	// 应该给每一个log entry一个唯一的序列号，这样就可以通过序列号来判断是否已经应用了这个log entry
	// 但是这个序列号是在client端生成的，所以需要在KVServer中维护一个clientID和sequenceNum
	// 用于判断是否已经应用了这个log entry
	logMap map[int64]int // 用于存储每个clientID对应的最大sequenceNum

	remainTasks int // 用于存储当前KVServer中还有多少任务没有完成
}

func (kv *KVServer) processLogEntry() {
	for {
		// 接收Leader需要应用的日志
		rawMsg := <-kv.applyCh
		if rawMsg.CommandValid {
			DPrintf(dLog, "S%v need to process log entry\n", rawMsg.ServerID)
			Msg := rawMsg.Command.(Op)
			result := Result{
				opType: Msg.Type,
				value:  "",
				err:    OK,
			}
			kv.mu.Lock()
			if Msg.Type == "Put" {
				// 如果是Put操作
				kv.data[rawMsg.ServerID][Msg.Key] = Msg.Value
			} else if Msg.Type == "Append" {
				// 如果是Append操作
				kv.data[rawMsg.ServerID][Msg.Key] += Msg.Value
			} else if Msg.Type == "Get" {
				// 如果是Get操作，则需要判断是否存在该键
				if value, ok := kv.data[rawMsg.ServerID][Msg.Key]; ok {
					result.value = value
				} else {
					result.err = ErrNoKey
				}
			}

			if kv.logMap[Msg.ClientID] == 1 {
				DPrintf(dLog, "S%v need to notify client\n", rawMsg.ServerID)
				kv.logMap[Msg.ClientID] = 0
				kv.mu.Unlock()
				kv.processCh <- result
				continue
			} else {
				DPrintf(dLog, "S%v client has already been notified\n", rawMsg.ServerID)
			}
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
			// kv.mu.Lock()
			// kv.remainTasks--
			// DPrintf(dLog2, "KVServer remainTasks %v\n", kv.remainTasks)
			// kv.mu.Unlock()
			return Msg
		case <-timer.C:
			kv.mu.Lock()
			curTerm, isLeader := kv.rf.GetState()
			kv.mu.Unlock()
			if curTerm != startTerm || !isLeader {
				return Result{
					opType: "",
					value:  "",
					err:    ErrWrongLeader,
				}
			}
			timer.Reset(1000 * time.Millisecond)
			DPrintf(dLog, "KVServer WaitApplyCh timeout\n")
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	kv.mu.Unlock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.rf.Start(Op{
		Key:         args.Key,
		Value:       "",
		ClientID:    args.ClientID,
		Type:        "Get",
		SequenceNum: args.SequenceNum,
	})
	// 等待日志提交
	kv.mu.Lock()
	kv.logMap[args.ClientID] = 1
	// kv.remainTasks++
	kv.mu.Unlock()
	result := kv.WaitApplyCh()
	reply.Err = result.err
	reply.Value = result.value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.rf.Start(Op{
		Key:         args.Key,
		Value:       args.Value,
		ClientID:    args.ClientID,
		Type:        args.Op,
		SequenceNum: args.SequenceNum,
	})
	// 等待日志提交
	kv.mu.Lock()
	kv.logMap[args.ClientID] = 1
	// kv.remainTasks++
	kv.mu.Unlock()
	result := kv.WaitApplyCh()
	reply.Err = result.err
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
	kv.data = make([]map[string]string, len(servers))
	for i := range kv.data {
		kv.data[i] = make(map[string]string)
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.processCh = make(chan Result)
	kv.logMap = make(map[int64]int)
	kv.remainTasks = 0
	go kv.processLogEntry()

	return kv
}
