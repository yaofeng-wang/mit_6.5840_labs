package raft

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d %d at term=%d received request vote args=%+v",
		MillisecondsPassed(rf.startTime),
		rf.me,
		rf.currentTerm,
		args)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = nil
		//DPrintf("%d %d becomes Follower: Term=%d", MillisecondsPassed(rf.startTime), rf.me, rf.currentTerm)
	}

	if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
		// check if candidate is at least as up-to-date
		if len(rf.logs) == 0 || args.LastLogTerm > rf.logs[len(rf.logs)-1].Term {
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
			rf.heartbeatCh <- struct{}{}
			DPrintf("%d %d gave vote to %d", MillisecondsPassed(rf.startTime), rf.me, args.CandidateId)
			return
		}

		if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs) {
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
			rf.heartbeatCh <- struct{}{}
			DPrintf("%d %d gave vote to %d", MillisecondsPassed(rf.startTime), rf.me, args.CandidateId)
			return
		}
	}
	//DPrintf("%d %d at term=%d did not vote for %d, already voted for %d",
	//	MillisecondsPassed(rf.startTime),
	//	rf.me,
	//	rf.currentTerm,
	//	args.CandidateId,
	//	*rf.votedFor)
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
	voteTerm := rf.currentTerm
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
			LastLogIndex: len(rf.logs),
		}
		if len(rf.logs) > 0 {
			args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		reply := &RequestVoteReply{}
		rf.mu.Unlock()

		go func(index int, args *RequestVoteArgs, reply *RequestVoteReply) {
			if success := rf.sendRequestVote(index, args, reply); !success {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = nil
				rf.state = follower
				return
			}
			if reply.VoteGranted {
				numVotes++
			}
			if rf.state != leader && (numVotes<<1) > len(rf.peers) && rf.currentTerm == voteTerm {
				rf.state = leader
				rf.nextIndices = make([]int, len(rf.peers))
				for i := range rf.nextIndices {
					rf.nextIndices[i] = len(rf.logs) + 1
				}
				rf.matchIndices = make([]int, len(rf.peers))
				DPrintf("%d %d becomes leader at term=%v, numVotes=%v, len(rf.peers)=%v", MillisecondsPassed(rf.startTime), rf.me, rf.currentTerm, numVotes, len(rf.peers))
				go rf.sendHeartbeats()
			}
		}(i, args, reply)
	}
}
