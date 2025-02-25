package kvraft

import (
	"MyRaft/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID    int
	clientID    int64
	sequenceNum int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.leaderID = 0
	ck.clientID = nrand() // 为Clerk对象的clientId字段生成一个随机数
	ck.sequenceNum = -1
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) updateSequenceNum() int {
	ck.sequenceNum++
	return ck.sequenceNum
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:         key,
		ClientID:    ck.clientID,
		SequenceNum: ck.updateSequenceNum(),
	}
	reply := GetReply{
		Err:   "",
		Value: "",
	}

	for {
		server := ck.servers[ck.leaderID]
		DPrintf(dClient, "C%d: server %v, key %v, sequenceNum %v\n", ck.clientID, ck.leaderID, key, args.SequenceNum)
		ok := server.Call("KVServer.Get", &args, &reply)
		if ok {
			DPrintf(dState, "C%d: Get reply %v\n", ck.clientID, reply.Err)
			if reply.Err == OK {
				break
			} else if reply.Err == ErrWrongLeader {
				// 如果返回的错误是ErrWrongLeader，那么就更新leaderID，然后继续尝试
				ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			} else {
				// 如果返回的错误是ErrNoKey，那么就说明key不存在，直接返回空字符串
				return ""
			}
		} else {
			// 如果RPC调用失败，那么也更新leaderID，然后继续尝试
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientID:    ck.clientID,
		SequenceNum: ck.updateSequenceNum(),
	}

	reply := PutAppendReply{
		Err: "",
	}
	for {
		server := ck.servers[ck.leaderID]
		DPrintf(dClient, "C%d: server %v, key %v, value %v, sequenceNum %v\n", ck.clientID, ck.leaderID, key, value, args.SequenceNum)
		ok := server.Call("KVServer.PutAppend", &args, &reply)
		if ok {
			DPrintf(dState, "C%d: PutAppend reply %v\n", ck.clientID, reply.Err)
			if reply.Err == OK {
				break
			} else if reply.Err == ErrWrongLeader {
				ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			}
		} else {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
		time.Sleep(60 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
