package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d %d receives AppendEntries: term= %v args=%+v", MillisecondsPassed(rf.startTime), rf.me, rf.currentTerm, args)

	DPrintf("%d %d 1", MillisecondsPassed(rf.startTime), rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		go rf.ticker()
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = nil
		DPrintf("%d %d becomes Follower: Term=%d", MillisecondsPassed(rf.startTime), rf.me, rf.currentTerm)
	}
	DPrintf("%d %d 2", MillisecondsPassed(rf.startTime), rf.me)

	if args.PrevLogIndex > 0 {
		if len(rf.logs) < args.PrevLogIndex || args.PrevLogTerm != rf.logs[args.PrevLogIndex-1].Term {
			reply.Term = rf.currentTerm
			return
		}
	}

	DPrintf("%d %d 3", MillisecondsPassed(rf.startTime), rf.me)

	// TODO assumed length of Entries is always 1
	if len(args.Entries) == 1 &&
		len(rf.logs) >= args.PrevLogIndex+1 &&
		rf.logs[args.PrevLogIndex].Term != args.Entries[0].Term {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}

	rf.logs = append(rf.logs, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	DPrintf("%d %d Insert heartbeat", MillisecondsPassed(rf.startTime), rf.me)
	rf.heartbeatCh <- &args.Term
	DPrintf("%d %d Inserted heartbeat", MillisecondsPassed(rf.startTime), rf.me)

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
