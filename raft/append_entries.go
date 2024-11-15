package raft

import "time"

// 向单个服务器发送附加日志条目
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// 向所有服务器发送心跳
func (rf *Raft) heartBeat() {
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
		Entries:  nil,
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
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Entries == nil {
		// 心跳
		DPrintf(dInfo, "F%d receive heartbeat from L%d\n", rf.me, args.LeaderID)
		reply.Success = true
		rf.updateTime = time.Now()
		rf.state = Follower
	} else {
		// 非心跳，正常的日志条目
	}
}
