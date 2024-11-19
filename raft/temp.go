package raft

import (
	"sync"
	"time"
)

type AppendController struct {
	// 用于控制附加日志条目的RPC请求
	wg            sync.WaitGroup   // 用于等待所有的RPC请求完成
	receivedCount int              // 用于记录已经接收到的数量
	appendCount   int              // 用于记录已经append成功的数量
	appendCh      chan bool        // 用于通知RPC请求完成
	timeout       <-chan time.Time // 用于超时控制
}

func (rf *Raft) entriesToSingle(server int, appendCtrl *AppendController) {
	defer appendCtrl.wg.Done()
	if rf.state != Leader {
		return
	}
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
			// rf.nextIndex[server] += len(logEntry)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			// DPrintf(dSnap, "L%d F%d update nextIndex to %d\n", rf.me, server, rf.nextIndex[server])
			rf.mu.Unlock()
			appendCtrl.appendCh <- true
		} else if reply.Conflict {
			appendCtrl.appendCh <- false
			rf.nextIndex[server]--
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
					appendCtrl.appendCount++
				}
				if appendCtrl.appendCount > len(rf.peers)/2 {
					rf.mu.Lock()
					preCommitIndex := rf.commitIndex
					rf.commitIndex = rf.recvdIndex
					DPrintf(dCommit, "L%d commit success, commitIndex from %d to %d\n", rf.me, preCommitIndex, rf.commitIndex)
					rf.applyCondSignal()
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	me := rf.me
	if rf.state != Leader {
		rf.mu.Unlock()
		return 0, 0, false
	}

	curTerm, _ := rf.GetState()
	DPrintf(dInfo, "L%d nextIndex is %v\n", me, rf.nextIndex)
	rf.log = append(rf.log, LogEntry{Term: curTerm, Command: command})
	rf.recvdIndex++
	recvdIndex := rf.recvdIndex
	rf.nextIndex[me] = recvdIndex + 1
	rf.matchIndex[me] = recvdIndex
	DPrintf(dInfo, "L%d received command %v\n", me, command)
	rf.mu.Unlock()

	// 发送当前日志到所有服务器，防止在发送日志的过程中，又接收到了新的日志
	rf.entriesToAll()

	return recvdIndex, curTerm, true
}

func (rf *Raft) heartBeat() {
	rf.entriesToAll()
}
