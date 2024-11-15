package raft

import (
	"sync"
	"time"
)

// 向单个服务器发送附加日志条目
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// Your code here (2A, 2B).
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 向所有服务器发送心跳
func (rf *Raft) heartBeat() {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
		Entries:      nil,
	}
	reply := AppendEntriesReply{}
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}

// 处理附加日志条目的RPC请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 解决term冲突
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 心跳
	if args.Entries == nil {
		DPrintf(dInfo, "F%d receive heartbeat from L%d\n", rf.me, args.LeaderID)
		reply.Success = true
		rf.updateTime = time.Now()
		rf.state = Follower
	} else {
		// 非心跳，正常的日志条目
		// 如果日志在 prevLogIndex 处不匹配，则返回 false
		if len(rf.log) != args.PrevLogIndex+1 {
			DPrintf(dInfo, "F%d logLength is %d but Prevlogindex is %d\n", rf.me, len(rf.log), args.PrevLogIndex)
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}

		// 如果一个已经存在的条目和新条目在相同的索引位置有相同的任期号和索引值，则复制其后的所有条目
		rf.log = append(rf.log, args.Entries...)
	}

	// 如果 leaderCommit > commitIndex，将 commitIndex 设置为 leaderCommit 和已有日志条目索引的较小值
	if args.LeaderCommit > rf.commitIndex {
		preCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		DPrintf(dCommit, "F%d update commitIndex from %d to %d\n", rf.me, preCommitIndex, min(args.LeaderCommit, len(rf.log)-1))
		rf.applyCondSignal()
	}

	reply.Term = rf.currentTerm
	reply.Success = true

}

// 使用Raft的服务（例如k/v服务器）希望开始对要附加到Raft日志的下一个命令达成一致。
// 如果此服务器不是领导者，则返回false。否则，开始协议并立即返回。
// 不能保证此命令将被提交到Raft日志中，因为领导者可能会失败或丢失选举。
// 即使Raft实例已被终止，此函数也应优雅地返回。
//
// 第一个返回值是命令将出现的索引（如果它被提交的话）。
// 第二个返回值是当前任期。
// 第三个返回值是如果此服务器认为自己是领导者，则为true。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()

	// 如果此服务器不是领导者，则返回false
	if !isLeader {
		return index, term, isLeader
	}
	// 否则，接受command并将其附加到日志中，并发送到其他服务器上
	// 附加日志条目

	rf.mu.Lock()
	// 使用index和term记录添加日志之前的index和term
	index = rf.commitIndex
	term = rf.log[index].Term
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.mu.Unlock()

	var wg sync.WaitGroup
	commitCount := 1
	commitCh := make(chan bool, len(rf.peers)-1)
	timeout := time.After(rf.electionTimeout)

	for i := range rf.peers {
		if i != rf.me {
			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				rf.mu.Lock()
				// 如果 lastLogIndex ≥ 跟随者的 nextIndex：发送包含从 nextIndex 开始的日志条目的 AppendEntries RPC
				logEntry := rf.log[rf.nextIndex[server]:]
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					Entries:      logEntry,
					PrevLogIndex: index,
					PrevLogTerm:  term,
					LeaderCommit: index,
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(server, &args, &reply) {
					// 如果对方的Term更大，则更新自己的Term，转换为跟随者，将投票状态清空
					// 对应着论文中图二的rules for all servers
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = Follower
						rf.persist()
					}
					rf.mu.Unlock()
					// 如果收到投票结果，则将结果发送到voteCh中
					commitCh <- reply.Success
				} else {
					// 如果没有收到，则发送false到voteCh中
					commitCh <- false
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
				// 如果到一定时间都没有收到半数以上的服务器提交成功，则立即返回本领导者上一次提交的index和term
				DPrintf(dCommit, "L%d commit failed\n", rf.me)
				return index, term, isLeader
			case sucess, ok := <-commitCh:
				if !ok {
					commitCh = nil
				} else if sucess {
					commitCount++
				}
				if commitCount > len(rf.peers)/2 {
					// 如果提交成功，则更新index和term
					rf.mu.Lock()
					rf.commitIndex = len(rf.log) - 1
					index = rf.commitIndex
					term = rf.currentTerm
					rf.mu.Unlock()
					DPrintf(dCommit, "L%d commitIndex is %d\n", rf.me, index)
					rf.applyCondSignal()
					return index, term, isLeader
				}

			}
		} else {
			// 如果检测到在选举过程中由候选者变成了跟随者，例如任期原因，则立即退出选举
			isLeader = false
			DPrintf(dVote, "L%d become follower\n", rf.me)
			return index, term, isLeader
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
			preApplied := rf.lastApplied
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
			}
			rf.lastApplied = rf.commitIndex
			DPrintf(dInfo, "F%d applied log from %d to %d\n", rf.me, preApplied, rf.commitIndex)
		} else {
			// rf.applyCond.Wait() 调用了 c.L.Unlock() 来释放锁，然后进入等待状态。
			// 当条件满足时，线程会被唤醒并重新获取锁（通过 c.L.Lock()），然后继续执行。
			rf.applyCond.Wait()
		}
	}
}
