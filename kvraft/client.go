package kvraft

import (
	"MyRaft/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

// a个client
// b个KVServer，负责存储键值对，支持Get、Put、Append操作，向客户端返回键值对的值，向Server发起日志追加
// c个raft，其中只有一个是leader，负责存储日志，向KVServer返回追加结果
type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderID int   // 当前leader的ID
	clerkID  int64 // clerk的唯一ID
	seqNum   int   // 请求的序列号
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) updateSeqNum() int {
	// 为 Clerk 结构体分配递增的序列号（seqId）
	ck.seqNum += 1
	return ck.seqNum
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk) // 创建一个新的Clerk对象
	ck.leaderID = 0
	ck.servers = servers
	ck.clerkID = nrand() // 为Clerk对象的clientId字段生成一个随机数
	ck.seqNum = -1       // 将Clerk对象的seqId字段设置为-1
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:     key,               // 待获取的键值对的键
		ClerkId: ck.clerkID,        // Clerk 的唯一标识符
		SeqNum:  ck.updateSeqNum(), // 分配的序列号
	}

	reply := GetReply{}
	server := ck.leaderID
	for {
		DPrintf(dLog2, "C%d call [Get] request key=%s, seq=%d,", ck.clerkID, key, args.SeqNum)
		ok := ck.SendGet(server%len(ck.servers), &args, &reply) // 发送 Get 请求给指定的服务器
		if ok {
			if reply.Err == ErrWrongLeader { // 如果收到了 ErrWrongLeader 错误，表示当前服务器不是 Leader
				server += 1
				DPrintf(dLog2, "C%d ErrWrongLeader, retry server=%d", ck.clerkID, server%len(ck.servers))
				continue // 重试下一个服务器
			}
			ck.leaderID = server // 更新 Leader 的标识符
			DPrintf(dLog2, "C%d call [Get] response server=%d reply=%v", ck.clerkID, server%len(ck.servers), reply.Err)
			break // 获取到响应，退出循环
		} else {
			server += 1
		}
		time.Sleep(50 * time.Millisecond) // 等待一段时间后继续重试
	}

	return reply.Value // 返回获取到的键值对的值
}

func (ck *Clerk) PutAppend(key string, value string, opName string) {
	args := PutAppendArgs{
		Key:     key,               // 待写入或追加的键值对的键
		Value:   value,             // 待写入或追加的键值对的值
		Op:      opName,            // 操作类型，可以是 "Put" 或 "Append"
		ClerkId: ck.clerkID,        // Clerk 的唯一标识符
		SeqNum:  ck.updateSeqNum(), // 分配的序列号
	}
	reply := PutAppendReply{}
	server := ck.leaderID
	for {
		DPrintf(dLog2, "C%d call [PutAppend] request key=%s value=%s op=%s, seq=%d, server=%d", ck.clerkID, key, value, opName, args.SeqNum, server%len(ck.servers))
		ok := ck.SendPutAppend(server%len(ck.servers), &args, &reply) // 发送 PutAppend 请求给指定的服务器
		if ok {
			if reply.Err == ErrWrongLeader { // 如果收到了 ErrWrongLeader 错误，表示当前服务器不是 Leader
				server += 1
				time.Sleep(50 * time.Millisecond)
				DPrintf(dLog2, "C%d call [PutAppend] faild, try next server id =%d", ck.clerkID, server)
				continue // 重试下一个服务器
			}
			ck.leaderID = server // 更新 Leader 的标识符
			DPrintf(dLog2, "C%d call [PutAppend] response server=%d, reply = %v", ck.clerkID, server%len(ck.servers), reply.Err)
			break // 获取到响应，退出循环
		} else {
			server += 1
			DPrintf(dLog2, "C%d call [PutAppend] faild, try next server id =%d", ck.clerkID, server)
		}
		time.Sleep(50 * time.Millisecond) // 等待一段时间后继续重试
	}
}

func (ck *Clerk) SendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) SendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
