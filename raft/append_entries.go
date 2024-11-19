package raft

import (
	"sync"
	"time"
)

// 处理附加日志条目的RPC请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 解决term冲突
	me := rf.me
	reply.Success = false
	reply.Conflict = false
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		DPrintf(dInfo, "F%d become follower\n", me)
		reply.Term = args.Term
		return
	}

	// append entries rpc 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if rf.state == Candidate {
		rf.state = Follower
	}

	rf.updateTime = time.Now()
	// 如果日志在 prevLogIndex 处不匹配，则返回 false
	if args.PrevLogIndex > rf.recvdIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		if args.PrevLogIndex <= rf.recvdIndex {
			DPrintf(dInfo, "F%d MISMATCH lastTerm is %d but prevlogterm is %d\n", me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		} else {
			DPrintf(dInfo, "F%d MISMATCH lastIndex is %d but prevlogindex is %d\n", me, rf.recvdIndex, args.PrevLogIndex)
		}

		reply.Conflict = true // 说明日志不匹配
		reply.Term = rf.currentTerm
		return
	}

	// 如果一个已经存在的条目和新条目在相同的索引位置有相同的任期号和索引值，则复制其后的所有条目
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	// DPrintf(dInfo, "F%d update entry to %v [%d]\n", me, rf.log, rf.currentTerm)
	rf.recvdIndex = len(rf.log) - 1

	// 如果 leaderCommit > commitIndex，将 commitIndex 设置为 leaderCommit 和已有日志条目索引的较小值
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		// DPrintf(dCommit, "F%d update commitIndex to  %d [%d]\n", me, rf.commitIndex, rf.currentTerm)
		DPrintf(dCommit, "F%d update commitIndex to min(%d, %d)\n", me, args.LeaderCommit, len(rf.log)-1)
		rf.applyCondSignal()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.Conflict = false
	// DPrintf(dInfo, "F%d is called by L%d\n", me, args.LeaderID)
}

// 向单个服务器发送附加日志条目
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// Your code here (2A, 2B).
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 向所有服务器发送心跳，心跳需要包含其他服务器所缺少的日志，否则每次只能等到下次客户端请求时才能发送日志
func (rf *Raft) heartBeatv() {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.recvdIndex,
		PrevLogTerm:  rf.log[rf.recvdIndex].Term,
		Entries:      nil,
		IsHB:         true,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}

// 使用Raft的服务（例如k/v服务器）希望开始对要附加到Raft日志的下一个命令达成一致。
// 如果此服务器不是领导者，则返回false。否则，开始协议并立即返回。
// 不能保证此命令将被提交到Raft日志中，因为领导者可能会失败或丢失选举。
// 即使Raft实例已被终止，此函数也应优雅地返回。
//
// 第一个返回值是命令将出现的索引（如果它被提交的话）。
// 第二个返回值是当前任期。
// 第三个返回值是如果此服务器认为自己是领导者，则为true。
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
						DPrintf(dInfo, "L%d F%d refused because of network, try to resend heart is %d\n", rf.me, server, len(logEntry) == 0)
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
			DPrintf(dVote, "L%d become follower\n", me)
			return recvdIndex, currentTerm, false
		}
	}
}

func (rf *Raft) applyCondSignal() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) applyLog() {
	// 如果 commitIndex > lastApplied：增加 lastApplied，将 log[lastApplied] 应用到状态机，区分复制和应用日志条目
	// 例如，日志为set value = 2，则复制的意思是将这个日志条目复制到其他服务器上，只是记录这个操作
	// 而应用的意思是将这个日志条目应用到本地状态机上，例如，实际更改数据库中的value，将这个操作执行
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied {
			// 将日志应用到状态机
			// preApplied := rf.lastApplied
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
			}
			rf.lastApplied = rf.commitIndex
			// DPrintf(dInfo, "F%d applied log from %d to %d\n", me, preApplied, rf.commitIndex)
		} else {
			// rf.applyCond.Wait() 调用了 c.L.Unlock() 来释放锁，然后进入等待状态。
			// 当条件满足时，线程会被唤醒并重新获取锁（通过 c.L.Lock()），然后继续执行。
			rf.applyCond.Wait()
		}
	}
}
