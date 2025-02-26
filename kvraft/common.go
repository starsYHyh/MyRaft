package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type PutAppendArgs struct {
	Key     string
	Value   string
	Op      string // 操作类型（Put 或 Append）
	ClerkId int64
	SeqNum  int // 操作序号
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key     string
	ClerkId int64
	SeqNum  int // 操作序号
}

type GetReply struct {
	Err   Err
	Value string
}
