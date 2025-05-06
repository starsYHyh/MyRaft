package kvraft

import (
	"MyRaft/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

const (
	ChangeLeaderInterval = time.Millisecond * 20
)

// 客户端
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkID  int64
	leaderId int
}

// 用于生成一个随机数，可以生成clientId和commandId
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// 生成一个客户端
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkID = nrand()
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// 根据key获取value
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clerkID,
		CommandId: nrand(),
	}
	leaderId := ck.leaderId
	reply := GetReply{}
	for {
		if !ck.servers[leaderId].Call("KVServer.Get", &args, &reply) {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			return reply.Value
		case ErrNoKey:
			ck.leaderId = leaderId
			return ""
		case ErrTimeOut:
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clerkID,
		CommandId: nrand(),
	}
	leaderId := ck.leaderId
	reply := PutAppendReply{}
	for {
		if !ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply) {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			return
		case ErrNoKey:
			return
		case ErrTimeOut:
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
