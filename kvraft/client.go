package kvraft

import (
	"MyRaft/labrpc"
	"crypto/rand"
	"math/big"
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
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ""
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
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientID:    ck.clientID,
		SequenceNum: ck.updateSequenceNum(),
	}

	reply := PutAppendReply{}
	for {
		server := ck.servers[ck.leaderID]
		ok := server.Call("KVServer.PutAppend", &args, &reply)
		if ok {
			break
		}
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
	}
	ck.sequenceNum++

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
