package raft

func (rf *Raft) snapshotToSingle(server int, args *InstallSnapshotArgs, appendCtrl *AppendController) {
	defer appendCtrl.wg.Done()
	reply := InstallSnapshotReply{}

	if rf.sendInstallSnapshot(server, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.setNewTerm(reply.Term, -1)
			return
		}
		// 仍然需要判断当前任期是否已经过期
		if rf.currentTerm == args.Term {
			// 若appendCtrl.recvdIndex == args.LastIncludedIndex，说明补全快照之后，对方的日志已经是最新的了
			// 但是若appendCtrl.recvdIndex > args.LastIncludedIndex，说明补全快照之后，对方的日志还不是最新的，因为快照之后还有日志
			// 这里一定要给appendCh发送一个false，因为本轮是发送跟随者落后的快照
			// 在发送快照的情况下，不会有日志附加的操作
			// 因此，只能等到下一轮心跳时，再次发送日志，以实现全部日志的同步
			if args.LastIncludedIndex == appendCtrl.recvdIndex {
				appendCtrl.appendCh <- true
				return
			} else {
				appendCtrl.appendCh <- false
			}
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			rf.matchIndex[server] = args.LastIncludedIndex
		}
	} else {
		appendCtrl.appendCh <- false
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// 如果请求的任期大于当前任期，则更新任期
	// 然后更新快照
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term, args.LeaderID)
	}
	rf.resetTime()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.lastApplied = args.LastIncludedIndex
	rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	rf.commitIndex = args.LastIncludedIndex
	rf.recvdIndex = args.LastIncludedIndex
	rf.persistWithSnapshot()
	rf.applySnapshotFlag = true
	rf.applyCondSignal()
}
