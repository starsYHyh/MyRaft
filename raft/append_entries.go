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
	wg          sync.WaitGroup   // 用于等待所有的RPC请求完成
	appendCount int              // 用于记录已经append成功的数量
	appendCh    chan bool        // 用于通知RPC请求完成
	timeout     <-chan time.Time // 用于超时控制
	term        int              // 用于记录发出请求时的任期
	recvdIndex  int              // 用于记录当前接收到的最大日志索引
	commitIndex int              // 用于记录当前已经提交的最大日志索引
}

// 获得指定范围的日志条目
func (rf *Raft) getSlicedLog(startIndex int, endIndex int) []LogEntry {
	if endIndex == -1 {
		endIndex = rf.recvdIndex
	}
	return rf.log[startIndex-rf.lastIncludedIndex : endIndex-rf.lastIncludedIndex+1]
}

// 获得指定索引的日志条目
func (rf *Raft) getLogEntry(index int) LogEntry {
	if index == -1 {
		index = rf.recvdIndex
	}
	if index < rf.lastIncludedIndex {
		DPrintf(dError, "L%d getLogEntry error, index %d, lastIncludedIndex %d\n", rf.me, index, rf.lastIncludedIndex)
	}
	if index > rf.recvdIndex {
		DPrintf(dError, "L%d getLogEntry error, index %d, recvdIndex %d\n", rf.me, index, rf.recvdIndex)
	}
	return rf.log[index-rf.lastIncludedIndex]
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	me := rf.me
	if rf.state != Leader {
		return 0, 0, false
	}
	curTerm := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: curTerm, Command: command})
	rf.persistWithSnapshot()
	rf.recvdIndex++
	recvdIndex := rf.recvdIndex
	rf.nextIndex[me] = recvdIndex + 1
	rf.matchIndex[me] = recvdIndex

	// 发送当前日志到所有服务器，防止在发送日志的过程中，又接收到了新的日志
	rf.entriesToAll()
	return recvdIndex, curTerm, true
}

// 结合heartbeat和start，向所有服务器发送缺少的日志
func (rf *Raft) entriesToAll() {
	term := rf.currentTerm
	appendCtrl := AppendController{
		wg:          sync.WaitGroup{},
		appendCount: 1,
		appendCh:    make(chan bool, len(rf.peers)-1),
		timeout:     time.After(rf.heartBeatTime),
		term:        term,
		recvdIndex:  rf.recvdIndex,
		commitIndex: rf.commitIndex,
	}

	for i := range rf.peers {
		if i != rf.me {
			appendCtrl.wg.Add(1)
			// 2D，需要判断是否需要发送快照：nextIndex[i] <= lastIncludedIndex
			// 如果需要发送快照，则在本轮发送快照
			// 如果不需要发送快照，则发送日志，日志起点为log[nextIndex[i] - lastIncludedIndex]
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				// 将发送快照视为一种特殊的日志附加
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
				}
				DPrintf(dSnap, "L%d send snapshot to F%d, nextIndex %d, lastIncludedIndex %d\n", rf.me, i, rf.nextIndex[i], rf.lastIncludedIndex)
				go rf.snapshotToSingle(i, &args, &appendCtrl)

			} else {
				var logEntry []LogEntry = nil
				// 如果nextIndex[i] > recvdIndex
				// 则说明认为对方的日志已经是最新的了，所以不需要发送带有实际内容的日志
				if rf.nextIndex[i] <= rf.recvdIndex {
					logEntry = rf.getSlicedLog(rf.nextIndex[i], -1)
				}
				prevLogIndex := rf.nextIndex[i] - 1
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.getLogEntry(prevLogIndex).Term,
					Entries:      logEntry,
					LeaderCommit: rf.commitIndex,
				}
				// DPrintf(dTrace, "L%d send entries to F%d, nextIndex %d, recvdIndex %d, loglength %d\n", rf.me, i, rf.nextIndex[i], rf.recvdIndex, len(logEntry))
				go rf.entriesToSingle(i, &args, &appendCtrl)
			}
		}
	}

	go func() {
		appendCtrl.wg.Wait()
		close(appendCtrl.appendCh)
	}()
	go rf.waitAppendReply(&appendCtrl, term)
}

func (rf *Raft) entriesToSingle(server int, args *AppendEntriesArgs, appendCtrl *AppendController) {
	defer appendCtrl.wg.Done()
	reply := AppendEntriesReply{}

	if rf.sendAppendEntries(server, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.setNewTerm(reply.Term, -1)
			return
		}
		// 需要判断当前任期是否已经过期，因为在发送RPC请求的过程中，本服务器可能已经不是leader了
		if rf.currentTerm == args.Term {
			if reply.Success {
				// 有时候可能会连续重复发送相同的日志，导致nextIndex不断增加，所以需要取最小值
				tempNextIndex := rf.nextIndex[server]
				// rf.nextIndex[server] = min(rf.recvdIndex+1, rf.nextIndex[server]+len(args.Entries))
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				if tempNextIndex != rf.nextIndex[server] {
					// DPrintf(dInfo, "L%d update F%d nextIndex from %d to %d\n", rf.me, server, tempNextIndex, rf.nextIndex[server])
				}
				appendCtrl.appendCh <- true
			} else {
				rf.nextIndex[server] = reply.XIndex
				appendCtrl.appendCh <- false
			}
		}
	} else {
		// 如果是因为网络原因导致的失败，则等下次心跳或append时再次尝试
		appendCtrl.appendCh <- false
	}
}

func (rf *Raft) waitAppendReply(appendCtrl *AppendController, term int) {
	for {
		curTerm, isLeader := rf.GetState()
		if curTerm == term && isLeader {
			select {
			case success, ok := <-appendCtrl.appendCh:
				rf.mu.Lock()
				if rf.currentTerm != term || rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				if !ok {
					appendCtrl.appendCh = nil
				} else if success {
					appendCtrl.appendCount++
				}
				if appendCtrl.appendCount > len(rf.peers)/2 {
					// 因为在等待日志提交的过程中，可能有新的日志被leader接受，所以实际上commitIndex应当是旧的recvdIndex
					preCommitIndex := rf.commitIndex
					// rf.commitIndex = appendCtrl.recvdIndex
					rf.commitIndex = max(rf.commitIndex, appendCtrl.recvdIndex)
					if preCommitIndex != rf.commitIndex {
						// DPrintf(dCommit, "L%d commit success, commitIndex from %d to %d\n", rf.me, preCommitIndex, rf.commitIndex)
						rf.applyCondSignal()
					}
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			case <-appendCtrl.timeout:
				return
			}
		} else {
			return
		}
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

	// me := rf.me
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		// 将自己转换为follower，并且将voteFor设置为leaderID
		// 此处处理与其他不同，因为在其他地方收到的请求发出者不一定是leader
		// 但是在这里，收到的请求发出者一定是leader，因为只有leader才会发送附加日志条目的RPC请求
		rf.setNewTerm(args.Term, args.LeaderID)
	}

	if args.Term < rf.currentTerm {
		return
	}

	if rf.state == Candidate {
		rf.state = Follower
		rf.votedFor = args.LeaderID
	}

	rf.updateTime = time.Now()
	mPrevLogTerm := rf.lastIncludedTerm
	// newArgsPrevLogTerm := args.PrevLogTerm
	// newArgsPrevLogIndex := args.PrevLogIndex
	// 这一段代码是为了判断prevLogIndex、recvIndex和lastIncludedIndex的关系
	// 如果prevLogIndex <= recvIndex，则说明prevLogIndex处的日志已经存在
	// 如果prevLogIndex < lastIncludedIndex，则说明prevLogIndex处的日志在快照中，需要特殊处理
	// 因为无法获取到prevLogIndex处的日志，所以需要将prevLogIndex设置为lastIncludedIndex
	// 通过lastIncludedIndex和lastIncludedTerm来判断prevLogIndex处的日志是否匹配，如果不匹配，则返回false
	if args.PrevLogIndex <= rf.recvdIndex {
		if args.PrevLogIndex >= rf.lastIncludedIndex {
			mPrevLogTerm = rf.getLogEntry(args.PrevLogIndex).Term
		} else {
			// newArgsPrevLogIndex = rf.lastIncludedIndex
			// if newArgsPrevLogIndex-args.PrevLogIndex > len(args.Entries) {
			// 	DPrintf(dError, "F%d entries length error, prevLogIndex %d, lastIncludedIndex %d, entries length %d\n", me, args.PrevLogIndex, rf.lastIncludedIndex, len(args.Entries))
			// 	return
			// }
			// newArgsPrevLogTerm = args.Entries[newArgsPrevLogIndex-args.PrevLogIndex-1].Term

			alreadySnapshotLogLen := rf.lastIncludedIndex - args.PrevLogIndex
			// 如果已经快照截断的日志长度小于等于收到的Entries的长度，说明日志部分匹配，需要将截断的部分补充回来
			if alreadySnapshotLogLen <= len(args.Entries) {
				newArgs := &AppendEntriesArgs{
					Term:         args.Term,
					LeaderID:     args.LeaderID,
					PrevLogTerm:  rf.lastIncludedTerm,
					PrevLogIndex: rf.lastIncludedIndex,
					Entries:      args.Entries[alreadySnapshotLogLen:],
					LeaderCommit: args.LeaderCommit,
				}
				args = newArgs
			} else {
				// 如果已经快照截断的日志长度大于收到的Entries的长度，说明已经匹配，可以直接返回成功
				reply.Success = true
				return
			}
		}
	}

	// 如果日志在 prevLogIndex 处不匹配，则返回 false
	// 存在的问题是，无法处理prevLogIndex < lastIncludedIndex的情况，因为无法获取到prevLogIndex处的日志
	if args.PrevLogIndex > rf.recvdIndex || mPrevLogTerm != args.PrevLogTerm {
		if args.PrevLogIndex <= rf.recvdIndex {
			// 如果 prevLogIndex <= recvdIndex，则说明在prevLogIndex处，日志的任期不匹配，需要找到冲突的日志条目
			// 找到冲突条目的任期和该任期中它存储的第一个索引
			reply.XTerm = mPrevLogTerm
			xIndex := args.PrevLogIndex
			for ; xIndex >= rf.lastIncludedIndex; xIndex-- {
				if rf.getLogEntry(xIndex).Term != reply.XTerm {
					break
				}
			}
			// 若reply.XIndex == rf.lastIncludedIndex，则说明冲突的日志条目在快照中
			reply.XIndex = xIndex + 1
		} else {
			// 如果 prevLogIndex > recvdIndex，则说明本机缺少prevLogIndex处的日志，需要找到冲突的日志条目
			reply.XTerm = rf.currentTerm
			reply.XIndex = rf.recvdIndex + 1
		}

		reply.Term = rf.currentTerm
		return
	}

	// oldLogLength := len(rf.log)
	// 如果一个已经存在的条目和新条目在相同的索引位置有相同的任期号和索引值，则复制其后的所有条目
	for i, entry := range args.Entries {
		if args.PrevLogIndex+i+1 > rf.recvdIndex || rf.getLogEntry(args.PrevLogIndex+i+1).Term != entry.Term {
			rf.log = append(rf.getSlicedLog(rf.lastIncludedIndex, args.PrevLogIndex+i), args.Entries[i:]...)
			break
		}
	}

	// newLogLength := len(rf.log)
	rf.persistWithSnapshot()
	// preRecvIndex := rf.recvdIndex
	rf.recvdIndex = rf.lastIncludedIndex + len(rf.log) - 1
	// if preRecvIndex != rf.recvdIndex {
	// 	DPrintf(dInfo, "F%d update recvdIndex from %d to %d\n", me, preRecvIndex, rf.recvdIndex)
	// }
	// 如果 leaderCommit > commitIndex，将 commitIndex 设置为 leaderCommit 和已有日志条目索引的较小值
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.recvdIndex)
		// DPrintf(dCommit, "F%d update commitIndex to %d\n", me, rf.commitIndex)
		rf.applyCondSignal()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
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
		rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
		if rf.applySnapshotFlag {
			// 如果需要应用快照，则应用快照
			applyMsg := ApplyMsg{
				ServerID:      rf.me,
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotIndex: rf.lastIncludedIndex,
				SnapshotTerm:  rf.lastIncludedTerm,
			}
			rf.applySnapshotFlag = false
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else if rf.commitIndex > rf.lastApplied && rf.recvdIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				ServerID:      rf.me,
				CommandValid:  true,
				Command:       rf.getLogEntry(rf.lastApplied).Command,
				CommandIndex:  rf.lastApplied,
				SnapshotValid: false,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}
