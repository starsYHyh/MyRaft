package raft

import (
	"sync"
	"time"
)

func (rf *Raft) StartV2(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	// 如果此服务器不是领导者，则返回false
	me := rf.me
	if rf.state != Leader {
		rf.mu.Unlock()
		return 0, 0, false
	}
	// 否则，接受command并将其附加到日志中，并发送到其他服务器上
	// 附加日志条目

	// 将新的日志添加到日志中
	currentTerm, _ := rf.GetState()
	DPrintf(dInfo, "L%d nextIndex is %v\n", me, rf.nextIndex)
	rf.log = append(rf.log, LogEntry{Term: currentTerm, Command: command})
	rf.recvdIndex++
	recvdIndex := rf.recvdIndex
	rf.nextIndex[me] = recvdIndex + 1
	rf.matchIndex[me] = recvdIndex
	DPrintf(dInfo, "L%d received command %v\n", me, command)
	rf.mu.Unlock()

	var wg sync.WaitGroup
	commitCount := 1
	commitCh := make(chan bool, len(rf.peers)-1)
	timeout := time.After(rf.electionTimeout)

	for i := range rf.peers {
		if i != me {
			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				for {
					if rf.state != Leader || rf.recvdIndex < rf.nextIndex[server] {
						return
					}

					rf.mu.Lock()
					prevLogIndex := rf.nextIndex[server] - 1
					prevLogTerm := rf.log[prevLogIndex].Term
					logEntry := rf.log[prevLogIndex+1:]
					lastLogIndex := len(rf.log) - 1
					args := AppendEntriesArgs{
						Term:         currentTerm,
						LeaderID:     me,
						Entries:      logEntry,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						LeaderCommit: rf.commitIndex,
						IsHB:         false,
					}
					DPrintf(dInfo, "L%d send entry %v and commitIndex %d and prevLogIndex %d to F%d\n",
						me, logEntry, rf.commitIndex, prevLogIndex, server)
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					if rf.sendAppendEntries(server, &args, &reply) {
						// 如果对方的Term更大，则更新自己的Term，转换为跟随者，将投票状态清空
						// 对应着论文中图二的rules for all servers
						rf.mu.Lock()
						if reply.Term > currentTerm {
							rf.setNewTerm(reply.Term)
							rf.updateTime = time.Now()
							commitCh <- false
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						if reply.Success { // 如果成功，则更新nextIndex
							rf.mu.Lock()
							rf.nextIndex[server] = lastLogIndex + 1
							rf.matchIndex[server] = lastLogIndex
							DPrintf(dInfo, "F%d update nextIndex to %d\n", server, rf.nextIndex[server])
							rf.mu.Unlock()
							// 如果收到复制结果，则将结果发送到commitCh中
							commitCh <- true
							return
						} else { // 如果失败，则递减nextIndex，并不断重试
							rf.nextIndex[server]--
							DPrintf(dWarn, "L%d retry to send entry to F%d and start from %d\n", me, server, rf.nextIndex[server])
						}
					} else {
						DPrintf(dWarn, "L%d F%d refused because of network, try to resend heart is %d\n", rf.me, server, len(logEntry) == 0)
						commitCh <- false
					}
				}
			}(i)
		}
	}

	go func() {
		wg.Wait()
		close(commitCh)
	}()

	// 等待半数以上的服务器提交成功
	for {
		if rf.state == Leader {
			select {
			case <-timeout:
				// 如果到一定时间都没有收到半数以上的服务器提交成功，则立即返回
				DPrintf(dCommit, "L%d commit failed\n", me)
				return recvdIndex, currentTerm, true
			case sucess, ok := <-commitCh:
				if !ok {
					commitCh = nil
				} else if sucess {
					commitCount++
				}
				if commitCount > len(rf.peers)/2 {
					// 如果提交成功，则更新index和term
					rf.mu.Lock()
					rf.commitIndex = recvdIndex
					DPrintf(dCommit, "L%d update commitIndex to %d\n", me, rf.commitIndex)
					rf.applyCondSignal()
					rf.mu.Unlock()
					return recvdIndex, currentTerm, true
				}

			}
		} else {
			// 如果检测到在选举过程中由候选者变成了跟随者，例如任期原因，则立即退出选举
			DPrintf(dState, "L%d become follower\n", me)
			return recvdIndex, currentTerm, false
		}
	}
}

// 发起领导者选举
func (rf *Raft) leaderElectionV() {
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
	receivedCount := 1
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
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					// 如果收到投票结果，则将结果发送到voteCh中
					voteCh <- reply.VoteGranted
				} else {
					// 如果没有收到，则发送false到voteCh中
					// DPrintf(dWarn, "C%d send vote request to F%d failed\n", me, server)
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
				receivedCount++
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

					DPrintf(dState, "C%d become leader\n", me)
					// 成为领导者后，立即发送心跳
					rf.heartBeat()
					// 如果检测到自己成为了领导者，则立即退出选举
					return
				}
				if receivedCount == len(rf.peers) {
					// 如果收到了所有服务器的投票结果，但是没有通过，则立即退出选举
					rf.mu.Lock()
					rf.state = Follower
					rf.votedFor = -1
					DPrintf(dTimer, "C%d receive all vote but not enough and try to relection\n", me)
					rf.resetTime()
					rf.mu.Unlock()
					return
				}
			case <-timeout:
				// 如果定时器超时，则选举失败
				rf.state = Follower
				rf.votedFor = -1
				DPrintf(dTimer, "C%d election timeout\n", me)
				rf.resetTime()
				return
			}
		} else {
			// 如果检测到在选举过程中由候选者变成了跟随者，例如任期原因，则立即退出选举
			rf.state = Follower
			rf.votedFor = -1
			DPrintf(dVote, "C%d become follower\n", me)
			return
		}
	}
}
