package raft

import (
	"sync"
	"time"
)

type VoteController struct {
	// 用于控制投票的RPC请求
	wg        sync.WaitGroup   // 用于等待所有的RPC请求完成
	voteCount int              // 用于记录已经投票成功的数量
	voteCh    chan bool        // 用于通知RPC请求完成
	timeout   <-chan time.Time // 用于超时控制
	term      int              // 用于记录发出请求时的任期
}

func (rf *Raft) leaderElection() {
	// 设置当前节点为自己选举的候选人，并增加任期编号
	me := rf.me
	rf.currentTerm++
	rf.votedFor = me
	rf.persist()
	rf.state = Candidate
	rf.resetTime() // 重置选举超时计时器
	// 生成RequestVote RPC参数
	args := RequestVoteArgs{
		Term:         rf.currentTerm, // 当前任期
		CandidateID:  me,
		LastLogIndex: rf.recvdIndex,
		LastLogTerm:  rf.getLogEntry(rf.recvdIndex).Term,
	}
	DPrintf(dVote, "C%d start election, term is %d\n", me, rf.currentTerm)
	// 初始化投票控制结构
	voteCtrl := VoteController{
		wg:        sync.WaitGroup{},
		voteCount: 1,
		voteCh:    make(chan bool, len(rf.peers)-1),
		timeout:   time.After(rf.electionTimeout),
		term:      rf.currentTerm,
	}
	// 向其他服务器发送RequestVote RPC
	for i := range rf.peers {
		if i != me {
			voteCtrl.wg.Add(1)
			go rf.voteToSingle(i, &args, &voteCtrl)
		}
	}
	// 开辟一个goroutine，等待所有投票线程完成
	go func() {
		voteCtrl.wg.Wait()
		close(voteCtrl.voteCh)
	}()
	// 开辟一个goroutine，处理投票回复
	go rf.waitVoteReply(&voteCtrl)
}

// 向单个服务器发送RequestVote RPC，并处理回复
func (rf *Raft) voteToSingle(server int, args *RequestVoteArgs, voteCtrl *VoteController) {
	defer voteCtrl.wg.Done()
	reply := RequestVoteReply{}

	if rf.sendRequestVote(server, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 如果回复的任期大于当前任期，则更新当前任期
		if reply.Term > rf.currentTerm {
			rf.setNewTerm(reply.Term, -1)
			rf.resetTime()
			rf.updateTime = time.Now()
			voteCtrl.voteCh <- false
			return
		}
		voteCtrl.voteCh <- reply.VoteGranted
	} else {
		// 如果发送失败，则认为投票失败
		voteCtrl.voteCh <- false
	}
}

func (rf *Raft) waitVoteReply(voteCtrl *VoteController) {
	for {
		rf.mu.Lock()
		state, curTerm := rf.state, rf.currentTerm
		rf.mu.Unlock()
		// 如果当前任期已经改变或者不是候选人状态，则不再处理
		if state == Candidate && curTerm == voteCtrl.term {
			select {
			// 使用 select 语句来同时监听两个通道：
			// 投票通道: 接收其他节点的投票回复。
			// 超时通道: 检测选举是否超时。
			case voteGranted, ok := <-voteCtrl.voteCh:
				rf.mu.Lock()
				if rf.currentTerm != voteCtrl.term || rf.state != Candidate {
					rf.mu.Unlock()
					return
				}
				// 如果投票通道关闭 (!ok)，则将 voteCtrl.voteCh 置为 nil，
				// 表示不再接收新的投票。
				if !ok {
					voteCtrl.voteCh = nil
					rf.mu.Unlock()
					return
				} else if voteGranted {
					voteCtrl.voteCount++
				}
				// 如果获得了大多数的投票，则成为领导者
				if voteCtrl.voteCount > len(rf.peers)/2 {
					rf.state = Leader
					// 向其他服务器发送日志
					// 此日志不一定是空的，因为可能领导者和其他服务器之间的日志不一致
					for i := range rf.nextIndex {
						rf.nextIndex[i] = rf.recvdIndex + 1
						rf.matchIndex[i] = 0
					}
					rf.recvdIndex = rf.lastIncludedIndex + len(rf.log) - 1
					DPrintf(dState, "C%d become leader\n", rf.me)
					rf.entriesToAll()
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			case <-voteCtrl.timeout:
				rf.mu.Lock()
				if rf.currentTerm != voteCtrl.term || rf.state != Candidate {
					rf.mu.Unlock()
					return
				}
				rf.setNewTerm(rf.currentTerm, -1)
				rf.resetTime()
				rf.mu.Unlock()
				return
			}
		} else {
			return
		}
	}
}

// labrpc包模拟了一个有丢失的网络，在这个网络中，服务器可能无法访问，请求和回复可能丢失。
// Call()发送请求并等待回复。如果在超时间隔内收到回复，则Call()返回true；否则Call()返回false。因此，Call()可能一段时间不返回。
// 一个false返回可能是由于死服务器、无法访问的活服务器、丢失的请求或丢失的回复。
//
// Call()保证返回（也许有延迟）*除非*服务器端的处理程序函数不返回。因此，不需要在Call()周围实现自己的超时。
// 如果您在使RPC工作时遇到问题，请检查是否已经大写了通过RPC传递的结构中的所有字段名称，并且调用者是否使用&传递了回复结构的地址，而不是结构本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// RequestVote RPC 处理程序，响应候选人的请求
// 如果 term < currentTerm 就返回 false
// 如果 votedFor 为空或为 candidateId，并且候选者的日志至少和接收者一样新，就投票给候选者（§5.2, §5.4）
// 如果 RPC 请求或响应包含的任期 T > currentTerm：设置currentTerm = T，转换为跟随者（§5.1）。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果请求的任期小于当前任期
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// DPrintf(dVote, "F%d refuse vote for C%d because of term\n", rf.me, args.CandidateID)
		return
	}
	// 如果请求的任期大于当前任期
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term, -1)
	}

	recvdTerm := rf.getLogEntry(rf.recvdIndex).Term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		(args.LastLogTerm > recvdTerm ||
			(args.LastLogTerm == recvdTerm &&
				args.LastLogIndex >= rf.recvdIndex)) {
		// 如果 votedFor 为空或为 candidateId，并且候选者的日志至少和接收者一样新
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
		// DPrintf(dVote, "F%d vote for C%d because lastLogTerm is %d, rf.log[rf.recvdIndex].Term is %d, lastLogIndex is %d, rf.recvdIndex is %d\n",
		// 	rf.me, args.CandidateID, args.LastLogTerm, rf.log[rf.recvdIndex].Term, args.LastLogIndex, rf.recvdIndex)
		rf.updateTime = time.Now()
	} else {
		reply.VoteGranted = false
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			// DPrintf(dVote, "F%d refuse vote for %d and args.LastLogTerm is %d, rf.log[rf.recvdIndex].Term is %d, args.LastLogIndex is %d, rf.recvdIndex is %d\n",
			// 	rf.me, args.CandidateID, args.LastLogTerm, rf.log[rf.recvdIndex].Term, args.LastLogIndex, rf.recvdIndex)
		}
	}
	reply.Term = rf.currentTerm
}
