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
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6_5840/labgob"
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

	PersistentState

	commitIndex int
	lastApplied int

	state        State
	nextIndices  []int
	matchIndices []int

	heartbeatCh   chan struct{}
	applyCh       chan ApplyMsg
	startElection bool

	// for debugging
	startTime time.Time
}

type PersistentState struct {
	CurrentTerm int
	VotedFor    *int
	Logs

	LastIncludedIndex int
	LastIncludedTerm  int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.PersistentState)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persistentState PersistentState
	if d.Decode(&persistentState) != nil {
		DPrintf("%d %d failed to read persistentState", MillisecondsPassed(rf.startTime), rf.me)
	} else {
		rf.PersistentState = persistentState
	}
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d %d snapshot to index=%v", MillisecondsPassed(rf.startTime), rf.me, index)
	if rf.hasLogAt(index) {
		rf.LastIncludedTerm = rf.logAt(index).Term
		rf.deleteLogsToIndex(index)
		rf.LastIncludedIndex = index
	} else {
		DPrintf("%d %d failed to snapshot to index=%v logs=%+v "+
			"LastIncludedIndex=%v LastIncludedTerm=%v",
			MillisecondsPassed(rf.startTime), rf.me, index, rf.Logs, rf.LastIncludedIndex,
			rf.LastIncludedTerm)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return -1, -1, false
	}
	defer rf.persist()

	index := rf.lastLogIndex() + 1
	rf.appendLogs(logEntry{Term: rf.CurrentTerm, Command: command})
	DPrintf("%d %d received new log at index=%v", MillisecondsPassed(rf.startTime), rf.me, index)
	return index, rf.CurrentTerm, true
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-time.After(rf.getElectionTimeout()):
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.state == follower {
				rf.state = candidate
			}

			if rf.state == candidate {
				rf.CurrentTerm++
				rf.persist()
				rf.VotedFor = &rf.me
				go rf.requestVotes()
			}
			rf.mu.Unlock()
		case <-rf.heartbeatCh:

		}
	}
	DPrintf("%d %d killed", MillisecondsPassed(rf.startTime), rf.me)
}

// MakeRaft the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func MakeRaft(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, startTime time.Time) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.startTime = startTime
	rf.heartbeatCh = make(chan struct{})
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("%d %d ready", MillisecondsPassed(startTime), me)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyToStateMachine()

	return rf
}

func (rf *Raft) applyToStateMachine() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logAt(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintf("%d %d applied log index=%v", MillisecondsPassed(rf.startTime), rf.me, rf.lastApplied)
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
