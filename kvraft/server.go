package kvraft

import (
	"MyRaft/raft"
	"sync"
	"sync/atomic"
	"time"

	"MyRaft/labgob"
	"MyRaft/labrpc"
)

// 记录操作，其中包括由哪一个客户端发起，由哪一个KVServer处理，操作的序列号
type Op struct {
	Key      string
	Value    string
	Command  string // 操作的命令类型（Get、Put、Append）
	ClientID int64  // 客户端的唯一标识符
	ServerID int    // 服务端的标识符
	SeqNum   int    // 操作的序列号
}

type ClerkOps struct {
	seqNum      int     // 客户端当前操作序列号，每个客户端的操作序列号是独立的，以保持客户端自身的一致性
	getCh       chan Op // Get操作的通道，用于processMsg()函数处理完日之后将结果放到getCh通道中，与此同时，会在waitApplyMsgByCh()函数中等待getCh通道的返回
	putAppendCh chan Op // Put和Append操作的通道
	processCh   chan Op // 用于接收日志处理结果的通道
	msgUniqueId int     // RPC等待消息的唯一标识符，用于通知客户端，此字段全局唯一
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // 通过Kill()方法设置
	maxraftstate int   // 当日志增长到一定大小时进行快照

	data      map[string]string   // 存储键值对的数据源
	opsMap    map[int64]*ClerkOps // 客户端ID与ClerkOps结构体的映射表
	persister *raft.Persister     // 持久化存储
}

func (kv *KVServer) WaitApplyMsgByCh(ch chan Op, ck *ClerkOps) (Op, Err) {
	term, _ := kv.rf.GetState()                     // 获取当前服务器的任期号
	timer := time.NewTimer(1000 * time.Millisecond) // 创建一个定时器，设置超时时间为1秒
	for {
		select {
		case Msg := <-ch: // 从通道接收到消息
			return Msg, OK // 返回接收到的消息和OK错误码
		case <-timer.C: // 定时器超时
			curTerm, isLeader := kv.rf.GetState() // 获取当前服务器的任期号和领导状态
			if curTerm != term || !isLeader {     // 如果当前任期号不等于开始任期号，或者当前不是领导者
				kv.mu.Lock()
				ck.msgUniqueId = 0 // 将ClerkOps结构体的消息唯一标识符重置为0
				kv.mu.Unlock()
				return Op{}, ErrWrongLeader // 返回空的操作和ErrWrongLeader错误码
			}
			timer.Reset(1000 * time.Millisecond) // 重新设置定时器超时时间为1秒
		}
	}
}

func (kv *KVServer) NotifyApplyMsgByCh(ch chan Op, Msg Op) {
	// 如果通知超时，则忽略，因为客户端可能已经发送请求到另一个服务器
	timer := time.NewTimer(200 * time.Millisecond) // 创建一个定时器，设置超时时间为200毫秒
	select {
	case ch <- Msg: // 将消息发送到通道
		return
	case <-timer.C: // 定时器超时
		DPrintf(dLog, "KVServer%d NotifyApplyMsgByCh timeout", kv.me) // 打印超时日志
		return
	}
}

func (kv *KVServer) GetCk(ckId int64) *ClerkOps {
	ck, found := kv.opsMap[ckId] // 根据客户端ID从映射表中获取ClerkOps结构体
	if !found {                  // 如果未找到对应的ClerkOps结构体
		ck = new(ClerkOps)             // 创建一个新的ClerkOps结构体
		ck.seqNum = 0                  // 将序列号初始化为0
		ck.getCh = make(chan Op)       // 创建Get操作的通道
		ck.putAppendCh = make(chan Op) // 创建Put和Append操作的通道
		ck.processCh = make(chan Op)   // 创建用于接收日志处理结果的通道
		kv.opsMap[ckId] = ck           // 将新创建的ClerkOps结构体添加到映射表中
		DPrintf(dLog, "KVServer%d Init ck %d", kv.me, ckId)
	}
	return ck // 返回对应的ClerkOps结构体
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	ck := kv.GetCk(args.ClerkId) // 获取客户端的ClerkOps结构体，相当于存储着与客户端之间的连接
	// 开始一个命令
	logIndex, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Command:  "Get",
		ClientID: args.ClerkId,
		SeqNum:   args.SeqNum,
		ServerID: kv.me,
	})
	if !isLeader { // 如果当前不是领导者
		reply.Err = ErrWrongLeader // 设置错误码为ErrWrongLeader
		ck.msgUniqueId = 0         // 将ClerkOps结构体的消息唯一标识符重置为0
		kv.mu.Unlock()
		return
	}
	DPrintf(dLog, "KVServer%d Get %v, waiting logIndex=%d", kv.me, args, logIndex) // 打印日志，表示等待日志提交
	// 记录本次请求对应的日志索引——recvdIndex
	// 后续leader发送的ApplyMsg中的CommandIndex与recvdIndex比较，如果相等，则通知客户端
	ck.msgUniqueId = logIndex // 将当前命令的日志索引设置为ClerkOps结构体的消息唯一标识符
	kv.mu.Unlock()
	// 解析Op结构体
	rawMsg, err := kv.WaitApplyMsgByCh(ck.processCh, ck) // 等待从通道接收到Get操作的结果
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf(dLog, "KVServer%d Recived Msg [Get] SeqNum=%d", kv.me, args.SeqNum) // 打印日志，表示收到了Get操作的结果
	reply.Err = err
	if err != OK {
		// 领导者发生变更，返回ErrWrongLeader错误码
		return
	}

	_, foundData := kv.data[rawMsg.Key]
	if !foundData {
		reply.Err = ErrNoKey
		return
	} else {
		reply.Value = kv.data[rawMsg.Key]
		DPrintf(dLog, "KVServer%d Get %v, reply=%v", kv.me, args, reply.Err)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	ck := kv.GetCk(args.ClerkId) // 获取客户端的PutAppendArgs结构体
	// 已经处理过了
	if ck.seqNum > args.SeqNum { // 如果PutAppendArgs结构体的序列号大于请求的序列号
		kv.mu.Unlock()
		reply.Err = OK // 设置错误码为OK
		return
	}
	DPrintf(dLog, "KVServer%d PutAppend %v", kv.me, args) // 打印日志，表示执行了PutAppend操作
	// 开始一个命令
	logIndex, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		Command:  args.Op,
		ClientID: args.ClerkId,
		SeqNum:   args.SeqNum,
		ServerID: kv.me,
	})
	if !isLeader { // 如果当前不是领导者
		reply.Err = ErrWrongLeader // 设置错误码为ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ck.msgUniqueId = logIndex                                                            // 将当前命令的日志索引设置为ClerkOps结构体的消息唯一标识符
	DPrintf(dLog, "KVServer%d PutAppend %v, waiting logIndex=%d", kv.me, args, logIndex) // 打印日志，表示等待日志提交
	kv.mu.Unlock()
	// 第二步：等待通道
	reply.Err = OK                                    // 设置错误码为OK
	Msg, err := kv.WaitApplyMsgByCh(ck.processCh, ck) // 等待从通道接收到PutAppend操作的结果
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf(dLog, "KVServer%d Recived Msg [PutAppend] from ck.putAppendCh args=%v, SeqId=%d, Msg=%v", kv.me, args, args.SeqNum, Msg) // 打印日志，表示收到了PutAppend操作的结果
	reply.Err = err
	if err != OK {
		DPrintf(dLog, "KVServer%d PutAppend %v, reply=%v", kv.me, args, reply) // 打印日志，表示执行了PutAppend操作
		return
	}
}

func (kv *KVServer) processMsg() {
	for {
		applyMsg := <-kv.applyCh     // 从通道接收应用层提交的日志
		Msg := applyMsg.Command.(Op) // 解析日志中的Op结构体，类型断言

		kv.mu.Lock()
		ck := kv.GetCk(Msg.ClientID) // 获取客户端的ClerkOps结构体
		// 当前不处理该日志
		if Msg.SeqNum > ck.seqNum { // 如果日志的序列号大于ClerkOps结构体的序列号
			DPrintf(dLog, "KVServer%d Ignore Msg, Msg.SeqId > ck.seqId", kv.me) // 打印日志，表示忽略该日志
			kv.mu.Unlock()
			continue
		}
		_, isLeader := kv.rf.GetState()

		// 检查是否需要通知
		needNotify := ck.msgUniqueId == applyMsg.CommandIndex
		// 需要判断该请求是否为本KVServer发送给leader
		if Msg.ServerID == kv.me && isLeader && needNotify { // 如果当前服务器是领导者，并且需要通知客户端
			// 通知通道并重置时间戳
			// 避免重复通知的机制是，每次通知后将ck.msgUniqueId重置为0
			// 且仅在下次客户端发起请求时，才会重新设置ck.msgUniqueId
			ck.msgUniqueId = 0
			kv.NotifyApplyMsgByCh(ck.processCh, Msg) // 通过通道通知客户端
		}

		if Msg.SeqNum < ck.seqNum { // 如果日志的序列号小于ClerkOps结构体的序列号
			DPrintf(dLog, "KVServer%d Ignore Msg, Msg.SeqId < ck.seqId", kv.me) // 打印日志，表示忽略该日志
			kv.mu.Unlock()
			continue
		}

		// 与通知客户端的逻辑类似，对应的data也只需要在leader上进行操作
		switch Msg.Command {
		case "Put":
			kv.data[Msg.Key] = Msg.Value // 执行Put操作，将键值对写入数据源
		case "Append":
			kv.data[Msg.Key] += Msg.Value // 执行Append操作，将值追加到键对应的现有值后面
		}
		ck.seqNum++ // 更新ClerkOps结构体的序列号
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// 调用labgob.Register注册你想要Go的RPC库进行编组/解组的结构体。
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg, 1000)           // 创建一个用于接收raft模块应用层提交的日志的通道
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) // 创建一个raft对象
	kv.mu.Lock()
	kv.data = make(map[string]string)     // 创建一个用于存储键值对的数据源
	kv.opsMap = make(map[int64]*ClerkOps) // 创建一个用于存储客户端操作的消息映射表
	kv.persister = persister
	kv.mu.Unlock()
	go kv.processMsg() // 启动一个协程来处理接收到的日志
	return kv
}
