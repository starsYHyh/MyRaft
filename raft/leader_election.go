package raft

import (
	"sync"
	"time"
)

type VoteController struct {
	// 用于控制投票的RPC请求
	wg            sync.WaitGroup   // 用于等待所有的RPC请求完成
	receivedCount int              // 用于记录已经接收到的数量
	voteCount     int              // 用于记录已经投票成功的数量
	voteCh        chan bool        // 用于通知RPC请求完成
	timeout       <-chan time.Time // 用于超时控制
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	me := rf.me
	rf.currentTerm++
	rf.votedFor = me
	rf.state = Candidate
	rf.resetTime()

	args := RequestVoteArgs{
		Term:         rf.currentTerm, // 当前任期
		CandidateID:  me,
		LastLogIndex: rf.recvdIndex,
		LastLogTerm:  rf.log[rf.recvdIndex].Term,
	}
	DPrintf(dVote, "C%d start election, term is %d", me, rf.currentTerm)

	voteCtrl := VoteController{
		wg:            sync.WaitGroup{},
		voteCount:     1,
		receivedCount: 1,
		voteCh:        make(chan bool, len(rf.peers)-1),
		timeout:       time.After(rf.electionTimeout),
	}

	for i := range rf.peers {
		if i != me {
			voteCtrl.wg.Add(1)
			go rf.voteToSingle(i, &args, &voteCtrl)
		}
	}

	go func() {
		voteCtrl.wg.Wait()
		close(voteCtrl.voteCh)
	}()

	rf.mu.Unlock()

	for {
		if rf.state == Candidate {
			select {
			case voteGranted, ok := <-voteCtrl.voteCh:
				if rf.currentTerm != args.Term || rf.state != Candidate {
					return
				}
				voteCtrl.receivedCount++
				if !ok {
					voteCtrl.voteCh = nil
				} else if voteGranted {
					voteCtrl.voteCount++
				}

				if voteCtrl.voteCount > len(rf.peers)/2 {
					rf.mu.Lock()
					rf.state = Leader
					for i := range rf.nextIndex {
						rf.nextIndex[i] = rf.recvdIndex + 1
						rf.matchIndex[i] = 0
					}

					rf.recvdIndex = len(rf.log) - 1
					rf.mu.Unlock()

					DPrintf(dState, "C%d become leader\n", me)
					rf.entriesToAll()
					return
				}
				if voteCtrl.receivedCount == len(rf.peers) {
					rf.mu.Lock()
					rf.setNewTerm(rf.currentTerm)
					rf.resetTime()
					rf.mu.Unlock()
					return
				}
			case <-voteCtrl.timeout:
				if rf.currentTerm != args.Term || rf.state != Candidate {
					return
				}
				rf.mu.Lock()
				rf.setNewTerm(rf.currentTerm)
				rf.resetTime()
				rf.mu.Unlock()
				return
			}
		} else {
			return
		}
	}
}

func (rf *Raft) voteToSingle(server int, args *RequestVoteArgs, voteCtrl *VoteController) {
	defer voteCtrl.wg.Done()
	reply := RequestVoteReply{}

	if rf.sendRequestVote(server, args, &reply) {
		rf.mu.Lock()
		// 如果当前任期已经改变或者不是候选人状态，则不再处理
		if reply.Term > rf.currentTerm {
			rf.setNewTerm(reply.Term)
			rf.updateTime = time.Now()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		voteCtrl.voteCh <- reply.VoteGranted
	} else {
		voteCtrl.voteCh <- false
	}
}

// 向服务器发送RequestVote RPC，期望arg中的RPC参数，用RPC回复填充*reply，因此调用者应传递&reply。
// 传递给Call()的args和reply的类型必须与处理程序函数中声明的参数的类型相同（包括它们是否为指针）。
//
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	/*
		如果 term < currentTerm 就返回 false
		如果 votedFor 为空或为 candidateId，并且候选人的日志至少和接收者一样新，就投票给候选人（§5.2, §5.4）
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf(dVote, "F%d receive vote request from C%d\n", rf.me, args.CandidateID)
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf(dVote, "F%d refuse vote for C%d because of term\n", rf.me, args.CandidateID)
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		(args.LastLogTerm > rf.log[rf.recvdIndex].Term ||
			(args.LastLogTerm == rf.log[rf.recvdIndex].Term && args.LastLogIndex >= rf.recvdIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.updateTime = time.Now()
		DPrintf(dVote, "F%d vote for %d\n", rf.me, args.CandidateID)
		rf.persist()
	} else {
		reply.VoteGranted = false
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			DPrintf(dVote, "F%d refuse vote for C%d because of already have leader\n", rf.me, args.CandidateID)
		}
	}
	reply.Term = rf.currentTerm
}
