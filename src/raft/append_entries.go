package raft

import (
	"math/rand"
	"time"
)

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

	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.PrevLogIndex > 0 {
		if len(rf.logs) <= args.PrevLogIndex || args.PrevLogTerm != rf.logs[args.PrevLogIndex-1].Term {
			return
		}
	}

	// TODO assumed length of Entries is always 1
	//if len(args.Entries) == 1 &&
	//	len(rf.logs) >= args.PrevLogIndex+1 &&
	//	rf.logs[args.PrevLogIndex].Term != args.Entries[0].Term {
	//	rf.logs = rf.logs[:args.PrevLogIndex+1]
	//}
	//
	//rf.logs = append(rf.logs, args.Entries...)
	//
	//if args.LeaderCommit > rf.commitIndex {
	//	rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
	//}

	rf.heartbeatCh <- struct{}{}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(400+rand.Int63()%1000) * time.Millisecond
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {

		for i, _ := range rf.peers {

			if i == rf.me {
				continue
			}

			rf.mu.Lock()
			if rf.state != leader {
				rf.mu.Unlock()
				return
			}
			// TODO update other fields
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []logEntry{},
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock()

			go func(index int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
				if success := rf.sendAppendEntries(index, args, reply); !success {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.state = follower
					rf.currentTerm = reply.Term
					rf.votedFor = nil
					return
					//DPrintf("%d %d becomes Follower: Term=%d", MillisecondsPassed(rf.startTime), rf.me, rf.currentTerm)
				}
			}(i, args, reply)
		}
		time.Sleep(heartbeatInterval)
	}
}
