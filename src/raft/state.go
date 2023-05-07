package raft

type State int

const (
	follower State = iota
	candidate
	leader
)

func (rf *Raft) isFollower() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == follower
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == candidate
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == leader
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d %d becomes Leader: Term=%d", MillisecondsPassed(rf.startTime), rf.me, rf.currentTerm)
	rf.state = leader
}

func (rf *Raft) becomeFollower(newTerm *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != follower {
		DPrintf("%d %d becomes Follower: Term=%d", MillisecondsPassed(rf.startTime), rf.me, rf.currentTerm)
	}
	rf.state = follower
	if newTerm != nil && *newTerm > rf.currentTerm {
		rf.currentTerm = *newTerm
		rf.votedFor = nil
	}
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.state = candidate
	rf.votedFor = &rf.me
	DPrintf("%d %d becomes Candidate: Term=%d", MillisecondsPassed(rf.startTime), rf.me, rf.currentTerm)
}
