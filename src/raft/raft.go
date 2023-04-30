package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6_5840/labgob"
	"6_5840/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	heartbeatInterval = 100 * time.Millisecond // Must be gte 100ms
)

type State int

const (
	follower State = iota
	candidate
	leader
)

type logEntry struct {
	Term    int
	Command []byte
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    *int
	logs        []logEntry

	commitIndex int
	lastApplied int

	state        State
	nextIndices  []int
	matchIndices []int

	heartbeatCh chan *int
	applyCh     chan ApplyMsg
}

// TODO send request in parallel
func (rf *Raft) sendHeartbeats() {
	for rf.killed() == false {

		if !rf.isLeader() {
			return
		}

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			// TODO update other fields
			// TODO add locks in all functions that accesses internal values
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []logEntry{},
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(i, args, reply)
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(&reply.Term)
			}
		}
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == leader
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(1000+rand.Int63()%1000) * time.Millisecond
}

func (rf *Raft) getVotes(hasLeaderWithLargerTermCh chan<- *int) <-chan struct{} {
	gotMajorityVotesCh := make(chan struct{})
	go func() {
		numVotes := rf.requestVotes(hasLeaderWithLargerTermCh) + 1
		if hasMajority(numVotes, len(rf.peers)) {
			DPrintf("%d got majority vote", rf.me)
			gotMajorityVotesCh <- struct{}{}
			return
		}
		DPrintf("%d failed to get majority vote", rf.me)
	}()
	return gotMajorityVotesCh
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d becomeLeader: Term=%d", rf.me, rf.currentTerm)
	rf.state = leader
}

func (rf *Raft) becomeFollower(newTerm *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = follower
	if newTerm != nil {
		rf.currentTerm = *newTerm
	}
	DPrintf("%d becomeFollower: Term=%d", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.state = candidate
	rf.votedFor = &rf.me
	DPrintf("%d becomeCandidate: Term=%d", rf.me, rf.currentTerm)
}

// send request in parallel
func (rf *Raft) requestVotes(hasLeaderWithLargerTermCh chan<- *int) int {
	numVotes := 0
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.logs),
		}
		if len(rf.logs) > 0 {
			args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		reply := &RequestVoteReply{}
		rf.sendRequestVote(i, args, reply)
		if reply.Term > rf.currentTerm {
			hasLeaderWithLargerTermCh <- &reply.Term
			return 0
		}
		if reply.VoteGranted {
			numVotes++
		}
	}
	return numVotes
}

func hasMajority(numVotes int, numPeers int) bool {
	return numVotes<<1 > numPeers
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if (rf.votedFor == nil || *rf.votedFor == args.CandidateId) && args.LastLogIndex >= len(rf.logs) {
		reply.VoteGranted = true
		return
	}
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
	DPrintf("AppendEntries: me=%v, args=%+v", rf.me, args)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.PrevLogIndex > 0 {
		if len(rf.logs) < args.PrevLogIndex || args.PrevLogTerm != rf.logs[args.PrevLogIndex-1].Term {
			reply.Term = rf.currentTerm
			return
		}
	}

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
	rf.heartbeatCh <- &args.Term
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection(hasLeaderWithLargerTermCh chan *int) {
	for {
		rf.becomeCandidate()
		select {
		case newTerm := <-rf.heartbeatCh:
			rf.becomeFollower(newTerm)
			go rf.ticker()
			return
		case newTerm := <-hasLeaderWithLargerTermCh:
			rf.becomeFollower(newTerm)
			go rf.ticker()
			return
		case <-rf.getVotes(hasLeaderWithLargerTermCh):
			rf.becomeLeader()
			go rf.sendHeartbeats()
			return
		case <-time.After(rf.getElectionTimeout()):
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		hasLeaderWithLargerTermCh := make(chan *int, 1)
		select {
		case newTerm := <-rf.heartbeatCh:
			DPrintf("%d received heartbeat", rf.me)
			rf.becomeFollower(newTerm)
		case <-time.After(rf.getElectionTimeout()):
			DPrintf("%d starts election", rf.me)
			rf.startElection(hasLeaderWithLargerTermCh)
			return
		case <-hasLeaderWithLargerTermCh:
			return
		}
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.heartbeatCh = make(chan *int, 1)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
