package raft

import (
	"sync"
	"time"
)

// 发起领导者选举
func (rf *Raft) leaderElection() {
	// 初始化参数
	rf.mu.Lock()
	me := rf.me
	rf.currentTerm++
	rf.votedFor = me     // 为自己投票
	rf.state = Candidate // 转换为候选者
	rf.resetTime()       // 重置选举时间、更新时间
	args := RequestVoteArgs{
		Term:         rf.currentTerm, // 当前任期
		CandidateID:  me,
		LastLogIndex: rf.recvdIndex,
		LastLogTerm:  rf.log[rf.recvdIndex].Term,
	}
	DPrintf(dVote, "C%d start election, term is %d", me, rf.currentTerm)
	rf.mu.Unlock()

	// 需并行向其他服务器发送投票请求，要保证在收到半数以上的选票或者选举超时后立即退出
	var wg sync.WaitGroup
	voteCount := 1
	// 创建一个channel，用于接收其他服务器的投票结果，缓冲区大小为peers-1
	voteCh := make(chan bool, len(rf.peers)-1)
	// 创建一个定时器，用于选举超时
	timeout := time.After(rf.electionTimeout)
	for i := range rf.peers {
		if i != me {
			wg.Add(1)
			// 向其他服务器发送投票请求
			go func(server int) {
				defer wg.Done()
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply) {
					// 如果对方的Term更大，则更新自己的Term，转换为跟随者，将投票状态清空
					// 对应着论文中图二的rules for all servers
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.setNewTerm(reply.Term)
						rf.updateTime = time.Now()
						return
					}
					rf.mu.Unlock()
					// 如果收到投票结果，则将结果发送到voteCh中
					voteCh <- reply.VoteGranted
				} else {
					// 如果没有收到，则发送false到voteCh中
					voteCh <- false
				}
			}(i)
		}
	}

	go func() {
		wg.Wait()
		close(voteCh) // 关闭voteCh的写入端
	}()

	for {
		if rf.state == Candidate {
			select {
			case voteGranted, ok := <-voteCh:
				if !ok { // 如果voteCh中已经没有了数据
					voteCh = nil
				} else if voteGranted { // 否则，如果收到了投票
					voteCount++
				}
				if voteCount > len(rf.peers)/2 {

					rf.mu.Lock()
					rf.state = Leader
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}

					rf.recvdIndex = len(rf.log) - 1
					rf.mu.Unlock()

					DPrintf(dLeader, "C%d become leader\n", me)
					rf.heartBeat()
					// 如果检测到自己成为了领导者，则立即退出选举
					return
				}
			case <-timeout:
				// 如果定时器超时，则选举失败
				DPrintf(dTimer, "C%d election timeout\n", me)
				return
			}
		} else {
			// 如果检测到在选举过程中由候选者变成了跟随者，例如任期原因，则立即退出选举
			// DPrintf(dVote, "C%d become follower\n", me)
			return
		}
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

	// me := rf.me
	// 首先判断日志条目
	// 如果本服务器的日志没有对方的日志新，则直接成为跟随者
	// 如果本服务器的日志比对方的日志新，则直接返回false，
	// 如果两边的日志一样新，则判断任期
	// 如果对方的任期比本服务器的任期新，则更新任期，成为跟随者
	// 如果对方的任期比本服务器的任期旧，则直接返回false
	// 如果两边的任期一样，则直接更新日志
	// if (rf.log[rf.recvdIndex].Term > args.LastLogTerm) ||
	// 	(rf.log[rf.recvdIndex].Term == args.LastLogTerm &&
	// 		rf.recvdIndex > args.LastLogIndex) {
	// 	DPrintf(dDrop, "F%d receive appendEntries with older log\n", me)
	// 	// DPrintf(dDrop, "F%d receive appendEntries with lower term %d and my term is %d\n", me, args.Term, rf.currentTerm)
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = false
	// 	return
	// } else if (rf.log[rf.recvdIndex].Term < args.LastLogTerm) ||
	// 	(rf.log[rf.recvdIndex].Term == args.LastLogTerm &&
	// 		rf.recvdIndex < args.LastLogIndex) {
	// 	DPrintf(dDrop, "F%d receive appendEntries with newer log\n", me)
	// 	// DPrintf(dDrop, "F%d receive appendEntries with higher term %d and my term is %d\n", me, args.Term, rf.currentTerm)
	// 	rf.currentTerm = args.Term
	// 	rf.votedFor = -1
	// 	rf.state = Follower
	// 	rf.persist()
	// } else if args.Term < rf.currentTerm {
	// 	DPrintf(dDrop, "F%d receive appendEntries with lower term %d and my term is %d\n", me, args.Term, rf.currentTerm)
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = false
	// 	return
	// } else if args.Term > rf.currentTerm {
	// 	rf.currentTerm = args.Term
	// 	rf.votedFor = -1
	// 	rf.state = Follower
	// 	rf.persist()
	// }

	// if args.Term < rf.currentTerm &&
	// 	(rf.log[rf.recvdIndex].Term > args.LastLogTerm) ||
	// 	(rf.log[rf.recvdIndex].Term == args.LastLogTerm && rf.recvdIndex > args.LastLogIndex) {
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = false
	// 	return
	// } else {
	// 	rf.setNewTerm(args.Term)
	// }

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	DPrintf(dVote, "F%d receive vote request from C%d\n", rf.me, args.CandidateID)

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
	}
	reply.Term = rf.currentTerm
}
