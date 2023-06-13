package raft

type InstallSnapshotArgs struct {
	Term, LeaderId, LastIncludedIndex, LastIncludedTerm int
	Data                                                []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.CurrentTerm > args.Term {
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.state = follower
		rf.CurrentTerm = reply.Term
		rf.VotedFor = nil
	}

	DPrintf("%d %d received snapshot rf.LastIncludedIndex=%v args.LastIncludedIndex=%v",
		MillisecondsPassed(rf.startTime), rf.me, rf.LastIncludedIndex, args.LastIncludedIndex)

	if rf.LastIncludedIndex >= args.LastIncludedIndex {
		return
	}
	if rf.hasLogAt(args.LastIncludedIndex) && rf.lastLog().Term == args.LastIncludedTerm {
		rf.deleteLogsToIndex(args.LastIncludedIndex)
	} else {
		rf.deleteAllLogs()
	}
	rf.SnapshotLogs = args.Data
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < args.LastIncludedIndex {
		DPrintf("%d %d commits newer snapshot rf.LastIncludedIndex=%v rf.LastIncludedTerm=%v args.Data=%v",
			MillisecondsPassed(rf.startTime), rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm, args.Data)
		rf.commitIndex = args.LastIncludedIndex
		if rf.commitIndex == 0 {
			DPrintf("%d %d rf.commitIndex == 0", MillisecondsPassed(rf.startTime), rf.me)
		}
	}

	if rf.lastApplied < args.LastIncludedIndex {
		DPrintf("%d %d applies newer snapshot rf.LastIncludedIndex=%v rf.LastIncludedTerm=%v args.Data=%v",
			MillisecondsPassed(rf.startTime), rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm, args.Data)
		rf.lastApplied = args.LastIncludedIndex
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
	}

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
