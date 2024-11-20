package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
//   创造一个新的Raft服务器
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
//   开始对新的日志条目达成一致
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
//   询问Raft当前的任期，以及它是否认为自己是领导者
// type ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//	 每次有新的条目被提交到日志中，每个Raft对等方都应该向服务（或测试人员）发送一个ApplyMsg

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

// A Go object implementing a single Raft peer.
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

	state           int           // 服务器的状态（跟随者、候选人、领导者）
	commitIndex     int           // 已知已提交的最高日志条目的索引（初始化为 0，单调增加）
	lastApplied     int           // 应用于状态机的最高日志条目的索引（初始化为 0，单调增加）
	updateTime      time.Time     // 上次收到心跳的时间，或开始选举的时间
	electionTimeout time.Duration // 选举超时时间

	// 领导者服务器上的易失状态（选举后重新初始化）

	nextIndex     []int         // 对于每个服务器，发送到该服务器的下一个日志条目的索引（初始化为领导者最后一个日志索引 + 1）
	matchIndex    []int         // 对于每个服务器，已知在服务器上复制的最高日志条目的索引（初始化为 0，单调增加）
	heartBeatTime time.Duration // 心跳时间
}

// 返回当前任期和该服务器是否认为自己是领导者
func (rf *Raft) GetState() (int, bool) {
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

// 将Raft的持久状态保存到稳定存储中，以便在崩溃和重新启动后可以检索。
// 保存的状态包括当前任期号和已投票的候选者ID。
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
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.recvdIndex = len(log) - 1
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
	IsHB         bool       // 是否为心跳
}

type AppendEntriesReply struct {
	Term           int  // currentTerm，用于领导者自我更新
	Success        bool // 如果跟随者包含与 prevLogIndex 和 prevLogTerm 匹配的条目，则为 true
	Conflict       bool // 如果跟随者包含与 prevLogIndex 和 prevLogTerm 不匹配的条目，则为 true
	ConnflictTerm  int  // 冲突条目的任期
	ConnflictIndex int  // 该任期中存储的第一个索引
}

// 测试人员在每次测试后不会停止Raft创建的goroutine，但它确实调用了Kill()方法。
// 您的代码可以使用killed()来检查是否已调用Kill()。使用原子操作避免了锁的需要。
//
// 问题是长时间运行的goroutine使用内存，可能会消耗CPU时间，可能导致后续测试失败并生成令人困惑的调试输出。
// 任何具有长时间运行循环的goroutine都应调用killed()来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) setNewTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
	DPrintf(dState, "F%d become follower\n", rf.me)
	rf.persist()
}

// ticker goroutine在此对等方最近没有收到心跳时启动新选举。
// 在ticker中，需要处理两件事
// 1. 如果最近选举超时时间内没有收到心跳，则启动新选举
// 2. 如果是领导者，则发送心跳
func (rf *Raft) ticker() {
	for !rf.killed() {
		// 无论是何种状态，都先休眠heartBeatTime时间
		time.Sleep(rf.heartBeatTime)
		// 如果是领导者，则每过一个heartbeat时间发送一次心跳
		if rf.state == Leader {
			rf.entriesToAll()
		}

		// 如果是跟随者，则检测距离上次收到心跳的时间是否超过了选举超时时间
		// 如果超过了，则启动新选举
		if rf.state == Follower && time.Since(rf.updateTime) > rf.electionTimeout {
			rf.leaderElection()
		}
	}
}

// 服务或测试人员想要创建一个Raft服务器。所有Raft服务器（包括这个）的端口都在peers[]中。
// 该服务器的端口是peers[me]。所有服务器的peers[]数组的顺序相同。
// persister是这个服务器保存其持久状态的地方，如果有的话，它最初保存了最近保存的状态。
//
// applyCh是测试人员或服务期望Raft发送ApplyMsg消息的通道。
//
// Make()必须快速返回，因此它应该为任何长时间运行的工作启动goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.heartBeatTime = 120 * time.Millisecond                                 // 心跳时间，因测试要求每秒不多于10次/秒
	rf.electionTimeout = time.Duration(360+rand.Intn(360)) * time.Millisecond // 选举超时时间，大于论文中的300ms
	rf.updateTime = time.Now()
	rf.currentTerm = 0           // 任期号
	rf.votedFor = -1             // 未投票
	rf.log = make([]LogEntry, 1) // 索引从1开始
	rf.log[0] = LogEntry{0, nil} // 第0个日志条目为空
	rf.commitIndex = 0           // 已知已提交的最高日志条目的索引
	rf.recvdIndex = 0            // 已知收到的最后一个日志条目的索引

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	go rf.applyLog()

	return rf
}
