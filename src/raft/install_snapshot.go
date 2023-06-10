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

	if rf.CurrentTerm > args.Term {
		return
	}

	rf.LastIncludedTerm = max(rf.LastIncludedTerm, args.LastIncludedTerm)
	rf.LastIncludedIndex = max(rf.LastIncludedIndex, args.LastIncludedIndex)
	rf.deleteAllLogs()
	reply.Term = rf.CurrentTerm
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
