package raft

import (
	"sync"
	"time"
)

// 使用Raft的服务（例如k/v服务器）希望开始对要附加到Raft日志的下一个命令达成一致。
// 如果此服务器不是领导者，则返回false。否则，开始协议并立即返回。
// 不能保证此命令将被提交到Raft日志中，因为领导者可能会失败或丢失选举。
// 即使Raft实例已被终止，此函数也应优雅地返回。
//
// 第一个返回值是命令将出现的索引（如果它被提交的话）。
// 第二个返回值是当前任期。
// 第三个返回值是如果此服务器认为自己是领导者，则为true。
type AppendController struct {
	// 用于控制附加日志条目的RPC请求
	wg            sync.WaitGroup   // 用于等待所有的RPC请求完成
	receivedCount int              // 用于记录已经接收到的数量
	appendCount   int              // 用于记录已经append成功的数量
	appendCh      chan bool        // 用于通知RPC请求完成
	timeout       <-chan time.Time // 用于超时控制
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	me := rf.me
	if rf.state != Leader {
		rf.mu.Unlock()
		return 0, 0, false
	}

	curTerm, _ := rf.GetState()
	rf.log = append(rf.log, LogEntry{Term: curTerm, Command: command})
	rf.recvdIndex++
	recvdIndex := rf.recvdIndex
	rf.nextIndex[me] = recvdIndex + 1
	rf.matchIndex[me] = recvdIndex
	DPrintf(dClient, "L%d received command\n", me)
	rf.mu.Unlock()

	// 发送当前日志到所有服务器，防止在发送日志的过程中，又接收到了新的日志
	rf.entriesToAll()

	return recvdIndex, curTerm, true
}

// 结合heartbeat和start，向所有服务器发送缺少的日志
// 参数为调用前接收到的最大日志索引和当前任期
// 防止在发送日志的过程中，又接收到了新的日志，导致发送的日志为新的日志
func (rf *Raft) entriesToAll() {
	rf.mu.Lock()
	appendCtrl := AppendController{
		wg:            sync.WaitGroup{},
		appendCount:   1,
		receivedCount: 1,
		appendCh:      make(chan bool, len(rf.peers)-1),
		timeout:       time.After(rf.heartBeatTime),
	}

	for i := range rf.peers {
		if i != rf.me {
			appendCtrl.wg.Add(1)
			go rf.entriesToSingle(i, &appendCtrl)
		}
	}

	go func() {
		appendCtrl.wg.Wait()
		close(appendCtrl.appendCh)
	}()
	rf.mu.Unlock()

	for {
		if rf.state == Leader {
			select {
			case success, ok := <-appendCtrl.appendCh:
				appendCtrl.receivedCount++
				if !ok {
					appendCtrl.appendCh = nil
				} else if success {
					// 如果存在一个 N 使得 N > commitIndex，且大多数 matchIndex[i] ≥ N，并且 log[N].term == currentTerm：设置 commitIndex = N
					// var N int
					// rf.mu.Lock()
					// for N = len(rf.log) - 1; N > rf.commitIndex; N-- {
					// 	count := 0
					// 	for j := range rf.peers {
					// 		if rf.matchIndex[j] >= N {
					// 			count++
					// 		}
					// 	}
					// 	if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
					// 		rf.commitIndex = N
					// 		DPrintf(dCommit, "L%d commit success to %d\n", rf.me, rf.commitIndex)
					// 		rf.applyCondSignal()
					// 		break
					// 	}
					// }
					// rf.mu.Unlock()

					appendCtrl.appendCount++
				}

				if appendCtrl.appendCount > len(rf.peers)/2 {
					rf.mu.Lock()
					preCommitIndex := rf.commitIndex
					rf.commitIndex = rf.recvdIndex
					if preCommitIndex != rf.commitIndex {
						DPrintf(dCommit, "L%d commit success, commitIndex from %d to %d\n", rf.me, preCommitIndex, rf.commitIndex)
						rf.applyCondSignal()
					}
					rf.mu.Unlock()
					return
				}
				if appendCtrl.receivedCount == len(rf.peers) {
					return
				}
			case <-appendCtrl.timeout:
				return
			}
		} else {
			return
		}
	}
}

func (rf *Raft) entriesToSingle(server int, appendCtrl *AppendController) {
	defer appendCtrl.wg.Done()

	rf.mu.Lock()
	var logEntry []LogEntry
	if rf.nextIndex[server] > rf.recvdIndex {
		// 代表为心跳包
		logEntry = nil
	} else {
		// 代表为日志包
		logEntry = rf.log[rf.nextIndex[server]:]
	}
	prevLogIndex := rf.nextIndex[server] - 1
	// DPrintf(dLeader, "L%d send entry %v and commitIndex %d and prevLogIndex %d to F%d\n", rf.me, logEntry, rf.commitIndex, prevLogIndex, server)

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      logEntry,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.setNewTerm(reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			// 有时候可能会连续重复发送相同的日志，导致nextIndex不断增加，所以需要取最小值
			rf.nextIndex[server] = min(rf.recvdIndex+1, rf.nextIndex[server]+len(logEntry))
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.mu.Unlock()
			appendCtrl.appendCh <- true
		} else if reply.Conflict {
			appendCtrl.appendCh <- false
			rf.nextIndex[server] = reply.ConnflictIndex
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			appendCtrl.appendCh <- false
		}
		return
	} else {
		// 如果是因为网络原因导致的失败，则等下次心跳或append时再次尝试
		appendCtrl.appendCh <- false
	}
}

// 向单个服务器发送附加日志条目
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// Your code here (2A, 2B).
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
			DPrintf(dDrop, "F%d MISMATCH lastTerm is %d but prevlogterm is %d\n", me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
			// 找到冲突条目的任期和该任期中它存储的第一个索引
			reply.ConnflictTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term != reply.ConnflictTerm {
					reply.ConnflictIndex = i + 1
					break
				}
			}
		} else {
			DPrintf(dDrop, "F%d MISMATCH lastIndex is %d but prevlogindex is %d\n", me, rf.recvdIndex, args.PrevLogIndex)
			reply.ConnflictTerm = rf.currentTerm
			reply.ConnflictIndex = rf.recvdIndex + 1
		}

		DPrintf(dInfo, "F%d update nextIndex from %d to %d\n", me, args.PrevLogIndex+1, reply.ConnflictIndex)
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
		DPrintf(dCommit, "F%d update commitIndex to %d\n", me, rf.commitIndex)
		rf.applyCondSignal()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.Conflict = false
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
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
			}
			rf.lastApplied = rf.commitIndex
		} else {
			// rf.applyCond.Wait() 调用了 c.L.Unlock() 来释放锁，然后进入等待状态。
			// 当条件满足时，线程会被唤醒并重新获取锁（通过 c.L.Lock()），然后继续执行。
			rf.applyCond.Wait()
		}
	}
}
