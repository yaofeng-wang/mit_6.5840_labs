package raft

type RequestVoteArgs struct {
	Term, CandidateId, LastLogIndex, LastLogTerm int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote handles RequestVote RPC from a candidate.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("%d %d at term=%d received request vote args=%+v rf.LastIncludedIndex=%v rf.LastIncludedTerm=%v rf.lastLogIndex()=%v", MillisecondsPassed(rf.startTime),
		rf.me, rf.CurrentTerm, args, rf.LastIncludedIndex, rf.LastIncludedTerm, rf.lastLogIndex())

	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.state = follower
		rf.CurrentTerm = args.Term
		rf.VotedFor = nil
	}

	if rf.VotedFor == nil || *rf.VotedFor == args.CandidateId {
		// check if candidate is at least as up-to-date

		if rf.isEmpty() && (args.LastLogTerm > rf.LastIncludedTerm || (args.LastLogTerm == rf.LastIncludedTerm && args.LastLogIndex >= rf.LastIncludedIndex)) {
			rf.giveVote(args.CandidateId, reply)
			return
		}

		if lastLog := rf.lastLog(); !rf.isEmpty() && (args.LastLogTerm > lastLog.Term ||
			(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= rf.lastLogIndex())) {
			rf.giveVote(args.CandidateId, reply)
			return
		}
	}
}

func (rf *Raft) giveVote(candidateID int, reply *RequestVoteReply) {
	reply.VoteGranted = true
	rf.VotedFor = &candidateID
	rf.heartbeatCh <- struct{}{}
	DPrintf("%d %d gave vote to %d", MillisecondsPassed(rf.startTime), rf.me, candidateID)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// requestVote sends requests to peers in parallel.
func (rf *Raft) requestVotes() {
	numVotes := 1
	rf.mu.Lock()
	voteTerm := rf.CurrentTerm
	DPrintf("%d %d starts election at term=%v", MillisecondsPassed(rf.startTime), rf.me, voteTerm)
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		args := &RequestVoteArgs{
			Term:         voteTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastLogIndex(),
		}
		if !rf.isEmpty() {
			args.LastLogTerm = rf.lastLog().Term
		} else {
			args.LastLogTerm = rf.LastIncludedTerm
		}
		reply := &RequestVoteReply{}
		rf.mu.Unlock()

		go func(index int, args *RequestVoteArgs, reply *RequestVoteReply) {
			if success := rf.sendRequestVote(index, args, reply); !success {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()

			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = nil
				rf.state = follower
				return
			}
			if reply.VoteGranted {
				numVotes++
			}
			if rf.state != leader && (numVotes<<1) > len(rf.peers) && rf.CurrentTerm == voteTerm {
				rf.state = leader
				rf.nextIndices = make([]int, len(rf.peers))
				for i := range rf.nextIndices {
					rf.nextIndices[i] = rf.lastLogIndex() + 1
				}
				rf.matchIndices = make([]int, len(rf.peers))
				DPrintf("%d %d becomes leader at term=%v, numVotes=%v, len(rf.peers)=%v", MillisecondsPassed(rf.startTime), rf.me, rf.CurrentTerm, numVotes, len(rf.peers))
				go rf.sendHeartbeats()
			}
		}(i, args, reply)
	}
}
