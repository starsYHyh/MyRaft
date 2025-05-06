package kvraft

import (
	"MyRaft/labgob"
	"MyRaft/labrpc"
	"MyRaft/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const WaitCmdTimeOut = time.Millisecond * 500 // cmd执行超过这个时间，就返回timeout
const MaxLockTime = time.Millisecond * 10     // debug

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId     int64 //用来标识commandNotify
	CommandId int64
	ClientId  int64
	Key       string
	Value     string
	Method    string
}

type CommandResult struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	stopCh  chan struct{}

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandNotifyCh map[int64]chan CommandResult
	lastApplies     map[int64]int64 //k-v：ClientId-CommandId
	data            map[string]string

	//持久化
	persister *raft.Persister
}

func (kv *KVServer) removeCh(reqId int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.commandNotifyCh, reqId)
}

// 调用start向raft请求命令
func (kv *KVServer) waitCmd(op Op) (res CommandResult) {
	DPrintf("server %v wait cmd start,Op: %+v.\n", kv.me, op)

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan CommandResult, 1)
	kv.commandNotifyCh[op.ReqId] = ch
	kv.mu.Unlock()
	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()
	select {
	case <-kv.stopCh:
		DPrintf("stop ch waitCmd")
		res.Err = ErrServer
	case res = <-ch:
	case <-t.C:
		res.Err = ErrTimeOut
	}
	kv.removeCh(op.ReqId)
	return
}

// 处理Get rpc
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("server %v in rpc Get,args: %+v", kv.me, args)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		ReqId:     nrand(),
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		Key:       args.Key,
		Method:    "Get",
	}
	//等待命令执行
	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

// 处理Put rpc
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("server %v in rpc PutAppend,args: %+v", kv.me, args)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		ReqId:     nrand(),
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		Key:       args.Key,
		Value:     args.Value,
		Method:    args.Op,
	}
	//等待命令执行
	res := kv.waitCmd(op)
	reply.Err = res.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.stopCh)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 保存快照
func (kv *KVServer) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	//生成快照数据
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.data); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.lastApplies); err != nil {
		panic(err)
	}
	data := w.Bytes()
	// 向底层 raft 通知进行快照
	kv.rf.Snapshot(logIndex, data)
}

// 读取快照
// 两处调用：初始化阶段；收到Snapshot命令，即接收了leader的Snapshot
func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	//对数据进行同步
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData map[string]string
	var lastApplies map[int64]int64

	if d.Decode(&kvData) != nil ||
		d.Decode(&lastApplies) != nil {
		log.Fatal("kv read persist err!")
	} else {
		kv.data = kvData
		kv.lastApplies = lastApplies
	}
}

func (kv *KVServer) notifyWaitCommand(reqId int64, err Err, value string) {
	if ch, ok := kv.commandNotifyCh[reqId]; ok {
		ch <- CommandResult{
			Err:   err,
			Value: value,
		}
	}
}

// 应用每一条命令
func (kv *KVServer) handleApplyCh() {
	for {
		select {
		case <-kv.stopCh:
			DPrintf("get from stopCh,server-%v stop!", kv.me)
			return
		case cmd := <-kv.applyCh:
			//处理快照命令，读取快照的内容
			if cmd.SnapshotValid {
				DPrintf("%v get install sn,%v %v", kv.me, cmd.SnapshotIndex, cmd.SnapshotTerm)
				kv.mu.Lock()
				kv.readPersist(cmd.Snapshot)
				kv.mu.Unlock()
				continue
			}
			//处理普通命令
			if !cmd.CommandValid {
				continue
			}
			cmdIdx := cmd.CommandIndex
			op := cmd.Command.(Op)
			kv.mu.Lock()

			if op.Method == "Get" {
				//处理读
				if v, ok := kv.data[op.Key]; ok {
					kv.notifyWaitCommand(op.ReqId, OK, v)
				} else {
					kv.notifyWaitCommand(op.ReqId, ErrNoKey, "")
				}
			} else if op.Method == "Put" || op.Method == "Append" {
				//处理写
				//判断命令是否重复
				if op.CommandId != kv.lastApplies[op.ClientId] {
					switch op.Method {
					case "Put":
						kv.data[op.Key] = op.Value
						kv.lastApplies[op.ClientId] = op.CommandId
					case "Append":
						kv.data[op.Key] += op.Value
						kv.lastApplies[op.ClientId] = op.CommandId
					default:
						kv.mu.Unlock()
						panic("unknown method " + op.Method)
					}

				}
				//命令处理成功
				kv.notifyWaitCommand(op.ReqId, OK, "")
			} else {
				kv.mu.Unlock()
				panic("unknown method " + op.Method)
			}
			//每应用一条命令，就判断是否进行持久化
			kv.saveSnapshot(cmdIdx)
			kv.mu.Unlock()
		}

	}

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
	kv.persister = persister

	// You may need initialization code here.
	kv.lastApplies = make(map[int64]int64)
	kv.data = make(map[string]string)

	kv.stopCh = make(chan struct{})
	//读取快照
	kv.readPersist(kv.persister.ReadSnapshot())

	kv.commandNotifyCh = make(map[int64]chan CommandResult)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.handleApplyCh()

	return kv
}
