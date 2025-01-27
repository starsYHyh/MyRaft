package raft

import (
	"MyRaft/labgob"
	"MyRaft/labrpc"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// 当每个Raft对等方意识到连续的日志条目被提交时，
// 对等方应该通过传递给Make() 的applyCh将ApplyMsg发送到服务（或测试人员）在同一台服务器上，
// 将CommandValid设置为true，以指示ApplyMsg包含新提交的日志条目。
// 在第2D部分中，您将希望在applyCh上发送其他类型的消息（例如快照），
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	// 快照相关
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 日志条目结构定义
type LogEntry struct {
	Term    int         // 该条目的任期号
	Command interface{} // 该条目的指令
}

const (
	Follower  = iota // 跟随者
	Candidate        // 候选者
	Leader           // 领导者
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // 服务或测试人员希望Raft发送ApplyMsg消息的通道
	applyCond *sync.Cond          // 用于通知applyCh有新的日志条目需要应用

	// 根据图2的描述，Raft服务器必须维护什么状态
	// 所有服务器上的持久性状态（在响应 RPC 之前更新稳定存储）

	currentTerm int        // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	votedFor    int        // 在当前任期内获得投票的候选者的ID（如果没有则为 null）
	log         []LogEntry // 日志条目集；每个条目包含状态机命令以及领导者收到条目时的任期（第一个索引为 1）
	recvdIndex  int        // 已知收到的最后一个日志条目的索引，即为日志长度-1，（初始化为 0，单调增加）

	// 所有服务器上的易失性状态
	commitIndex     int           // 已知已提交的最高日志条目的索引（初始化为 0，单调增加）
	lastApplied     int           // 应用于状态机的最高日志条目的索引（初始化为 0，单调增加）
	state           int           // 服务器的状态（跟随者、候选人、领导者）
	updateTime      time.Time     // 上次收到心跳的时间，或开始选举的时间
	electionTimeout time.Duration // 选举超时时间

	// 领导者服务器上的易失状态（选举后重新初始化）
	nextIndex     []int         // 对于每个服务器，发送到该服务器的下一个日志条目的索引（初始化为领导者最后一个日志索引 + 1）
	matchIndex    []int         // 对于每个服务器，已知在服务器上复制的最高日志条目的索引（初始化为 0，单调增加）
	heartBeatTime time.Duration // 心跳超时时间
}

// 返回当前任期和该服务器是否认为自己是领导者
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// 从持久化存储中恢复之前的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		// error handling
		DPrintf(dError, "readPersist error\n")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// 重置更新时间、随机化选举超时时间
func (rf *Raft) resetTime() {
	rf.updateTime = time.Now()
	rf.electionTimeout = time.Duration(360+rand.Intn(360)) * time.Millisecond
}

// 服务想要切换到快照。只有在Raft没有更多最近的信息时才这样做，因为它在applyCh上通信快照。
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// 服务说它已经创建了一个快照，其中包含所有信息，直到包括索引。
// 这意味着服务不再需要日志通过（包括）该索引。Raft现在应该尽可能地修剪其日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVote RPC 参数结构
// 字段名必须以大写字母开头！
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选者的任期号
	CandidateID  int // 请求选票的候选者的ID
	LastLogIndex int // 候选者的最后日志条目的索引
	LastLogTerm  int //	候选者最后一次日志条目的任期（§5.4）
}

// RequestVote RPC 回复结构
// 字段名必须以大写字母开头！
type RequestVoteReply struct {
	Term        int  // currentTerm，供候选人自行更新
	VoteGranted bool // true 表示候选人获得选票
}

type AppendEntriesArgs struct {
	Term         int        // 领导者的任期号
	LeaderID     int        // 领导者的ID
	PrevLogIndex int        // 新日志条目之前的索引
	PrevLogTerm  int        // 新日志条目之前的任期
	Entries      []LogEntry // 要附加的日志条目
	LeaderCommit int        // 领导者的commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm，用于领导者自我更新
	Success bool // 如果跟随者包含与 prevLogIndex 和 prevLogTerm 匹配的条目，则为 true
	XTerm   int  // 冲突条目的任期
	XIndex  int  // 该任期中存储的第一个索引
	XLen    int  // 跟随者的日志长度
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) setNewTerm(term int, voteFor int) {
	rf.currentTerm = term
	rf.votedFor = voteFor
	rf.state = Follower
	// DPrintf(dState, "F%d become follower\n", rf.me)
	rf.persist()
}

// 在ticker中，需要处理两件事
// 1. 如果是领导者，则发送心跳
// 2. 如果最近选举超时时间内没有收到心跳，则启动新选举
func (rf *Raft) ticker() {
	for !rf.killed() {
		// 无论是何种状态，都先休眠heartBeatTime时间
		time.Sleep(rf.heartBeatTime)
		rf.mu.Lock()
		// 如果是领导者，则每过一个heartbeat时间发送一次心跳
		if rf.state == Leader {
			rf.entriesToAll()
		}

		// 如果是跟随者，则检测距离上次收到心跳的时间是否超过了选举超时时间
		// 如果超过了，则启动新选举
		if (rf.state == Follower || rf.state == Candidate) && time.Since(rf.updateTime) > rf.electionTimeout {
			rf.leaderElection()
		}
		rf.mu.Unlock()
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.log = make([]LogEntry, 1)
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.readPersist(persister.ReadRaftState())

	// DPrintf(dState, "F%d currentTerm is %d, votedFor is %d, log is %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	DPrintf(dState, "F%d currentTerm is %d, votedFor is %d\n", rf.me, rf.currentTerm, rf.votedFor)

	// 初始化状态
	rf.state = Follower
	// 心跳时间，因测试要求每秒不多于10次/秒
	// 选举超时时间，大于论文中的300ms，且需要随机化
	rf.heartBeatTime = 120 * time.Millisecond
	rf.electionTimeout = time.Duration(360+rand.Intn(360)) * time.Millisecond
	rf.updateTime = time.Now()

	rf.commitIndex = 0              // 已知已提交的最高日志条目的索引
	rf.recvdIndex = len(rf.log) - 1 // 已知收到的最后一个日志条目的索引

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	go rf.ticker()   // 开辟一个goroutine，用于处理心跳和选举
	go rf.applyLog() // 开辟一个goroutine，用于应用日志
	return rf
}
