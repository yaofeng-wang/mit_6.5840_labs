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

	defer func() {
		rf.heartbeatCh <- struct{}{}
	}()
	if args.PrevLogIndex > 0 &&
		(len(rf.logs) < args.PrevLogIndex || args.PrevLogTerm != rf.logs[args.PrevLogIndex-1].Term) {
		return
	}

	for i, entry := range args.Entries {
		logIndex := i + args.PrevLogIndex + 1
		if len(rf.logs) >= logIndex && rf.logs[logIndex-1].Term != entry.Term {
			rf.logs = rf.logs[:logIndex-1]
			break
		}
	}

	for i := range args.Entries {
		logIndex := i + args.PrevLogIndex + 1
		if logIndex > len(rf.logs) {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
	}

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
	isFirstHeartbeat := true
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
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
			if isFirstHeartbeat || (len(rf.logs) > 0 && rf.logs[len(rf.logs)-1].Term != rf.currentTerm) {
				isFirstHeartbeat = false
			} else if len(rf.logs) >= rf.nextIndices[i] {
				args.PrevLogIndex = rf.nextIndices[i] - 1
				if args.PrevLogIndex != 0 {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
				}
				args.Entries = rf.logs[args.PrevLogIndex:]
			}

			reply := &AppendEntriesReply{}
			rf.mu.Unlock()

			go func(index int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
				for numTries := 0; numTries < 3; numTries++ {
					if success := rf.sendAppendEntries(index, args, reply); !success {
						return
					}
					rf.mu.Lock()

					if reply.Term > rf.currentTerm {
						rf.state = follower
						rf.currentTerm = reply.Term
						rf.votedFor = nil
						rf.mu.Unlock()
						return
						//DPrintf("%d %d becomes Follower: Term=%d", MillisecondsPassed(rf.startTime), rf.me, rf.currentTerm)
					}

					if reply.Success {
						rf.matchIndices[index] = max(rf.matchIndices[index], args.PrevLogIndex+len(args.Entries))
						rf.nextIndices[index] = rf.matchIndices[index] + 1

						origCommitIndex := rf.commitIndex
						for i := rf.commitIndex + 1; i <= rf.matchIndices[index]; i++ {
							count := 1
							for j := range rf.peers {
								if j == rf.me {
									continue
								}
								if rf.matchIndices[j] >= i {
									count++
								}
							}
							if ((count << 1) > len(rf.peers)) && (rf.logs[i-1].Term == rf.currentTerm) {
								rf.commitIndex = i
							}
						}
						if origCommitIndex != rf.commitIndex {
							DPrintf("%d %d commitIndex=%v", MillisecondsPassed(rf.startTime), rf.me, rf.commitIndex)
						}

						rf.mu.Unlock()
						break
					} else {
						rf.nextIndices[index]--
						rf.mu.Unlock()
					}
				}

			}(i, args, reply)
		}
		time.Sleep(heartbeatInterval)
	}
}
