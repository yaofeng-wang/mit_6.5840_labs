package raft

import (
	"math/rand"
	"time"
)

type AppendEntriesArgs struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm, LeaderCommit int
	Entries                                                 Logs
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm, XIndex, XLen int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("%d %d receives AppendEntries: term=%v args=%+v rf.lastLogIndex()=%v rf.commitIndex=%v",
		MillisecondsPassed(rf.startTime), rf.me, rf.CurrentTerm, args, rf.lastLogIndex(), rf.commitIndex)

	if args.Term > rf.CurrentTerm {
		rf.state = follower
		rf.CurrentTerm = args.Term
		rf.VotedFor = nil
	}
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		return
	}

	defer func() {
		DPrintf("%d %d try to send heartbeat", MillisecondsPassed(rf.startTime), rf.me)
		rf.heartbeatCh <- struct{}{}
		DPrintf("%d %d sent heartbeat", MillisecondsPassed(rf.startTime), rf.me)
	}()
	if args.PrevLogIndex > 0 {
		if rf.lastLogIndex() < args.PrevLogIndex {
			reply.XLen = rf.length()
			DPrintf("%d %d logs too short XLen=%v", MillisecondsPassed(rf.startTime),
				rf.me,
				reply.XLen)
			return
		} else if rf.hasLogAt(args.PrevLogIndex) && args.PrevLogTerm != rf.logAt(args.PrevLogIndex).Term {
			reply.XTerm = rf.logAt(args.PrevLogIndex).Term
			firstEntryOfTerm := 1
			for i := 0; i-1 < rf.length(); i++ {
				if rf.logAt(i+1).Term == reply.XTerm {
					firstEntryOfTerm = i + 1
					break
				}
			}
			reply.XIndex = firstEntryOfTerm
			reply.XLen = rf.length()
			DPrintf("%d %d conflict at PrevLog XTerm=%v, XIndex=%v",
				MillisecondsPassed(rf.startTime),
				rf.me,
				reply.XTerm,
				reply.XIndex)
			return
		}
	}

	for i, entry := range args.Entries {
		logIndex := i + args.PrevLogIndex + 1
		if rf.hasLogAt(logIndex) && rf.logAt(logIndex).Term != entry.Term {
			rf.deleteLogsFromIndex(logIndex)
			break
		}
	}

	for i := range args.Entries {
		logIndex := i + args.PrevLogIndex + 1
		if logIndex > rf.lastLogIndex() {
			rf.appendLogs(args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
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
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
			DPrintf("%d %d nextIndices[%v]=%v", MillisecondsPassed(rf.startTime), rf.me, i, rf.nextIndices[i])
			if isFirstHeartbeat || (!rf.isEmpty() && rf.lastLog().Term != rf.CurrentTerm) {
				isFirstHeartbeat = false
				args.PrevLogIndex = rf.commitIndex
				if rf.hasLogAt(args.PrevLogIndex) {
					args.PrevLogTerm = rf.logAt(args.PrevLogIndex).Term
				} else if args.PrevLogIndex == rf.LastIncludedIndex {
					args.PrevLogTerm = rf.LastIncludedTerm
				} else {
					DPrintf("%d %d [Error]PrevLogIndex is in snapshot args.PrevLogIndex=%v rf.LastIncludedIndex=%v", MillisecondsPassed(rf.startTime), rf.me, args.PrevLogIndex, rf.LastIncludedIndex)
				}
			} else {
				args.PrevLogIndex = rf.nextIndices[i] - 1
				if rf.hasLogAt(args.PrevLogIndex) {
					args.PrevLogTerm = rf.logAt(args.PrevLogIndex).Term
					args.Entries = rf.logsFromIndex(rf.nextIndices[i])
				} else if args.PrevLogIndex == rf.LastIncludedIndex {
					args.PrevLogTerm = rf.LastIncludedTerm
					args.Entries = rf.logsFromIndex(rf.nextIndices[i])
				} else {
					go func(i int) {
						DPrintf("%d %d PrevLogIndex is in snapshot args.PrevLogIndex=%v rf.LastIncludedIndex=%v", MillisecondsPassed(rf.startTime), rf.me, args.PrevLogIndex, rf.LastIncludedIndex)
						rf.mu.Lock()
						args := &InstallSnapshotArgs{
							Term: rf.CurrentTerm,
						}
						reply := &InstallSnapshotReply{}
						rf.mu.Unlock()
						for ii := 0; ii < 9; ii++ {
							if ok := rf.sendInstallSnapshot(i, args, reply); ok {
								rf.mu.Lock()
								if reply.Term > rf.CurrentTerm {
									rf.state = follower
									rf.CurrentTerm = reply.Term
									rf.VotedFor = nil
									rf.persist()
								}
								rf.mu.Unlock()
								return
							}
						}
					}(i)
					continue
				}
			}

			rf.mu.Unlock()

			go func(index int, args *AppendEntriesArgs) {
				for ii := 0; ii < 9; ii++ {
					reply := &AppendEntriesReply{}
					if success := rf.sendAppendEntries(index, args, reply); !success {
						return
					}

					rf.mu.Lock()
					if reply.Term < rf.CurrentTerm {
						rf.mu.Unlock()
						return
					}

					if reply.Term > rf.CurrentTerm {
						rf.state = follower
						rf.CurrentTerm = reply.Term
						rf.VotedFor = nil
						rf.persist()
						rf.mu.Unlock()
						return
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
							if ((count << 1) > len(rf.peers)) && (rf.logAt(i).Term == rf.CurrentTerm) {
								rf.commitIndex = i
							}
						}
						if origCommitIndex != rf.commitIndex {
							DPrintf("%d %d commitIndex=%v", MillisecondsPassed(rf.startTime), rf.me, rf.commitIndex)
						}

						rf.mu.Unlock()
						break
					} else {
						lastEntryWithSmallerTerm := rf.LastIncludedIndex + 1
						for i := 0; rf.hasLogAt(i) && rf.logAt(i).Term < reply.XTerm; i++ {
							lastEntryWithSmallerTerm = i + 1
						}

						rf.nextIndices[index] = lastEntryWithSmallerTerm
						args.PrevLogIndex = rf.nextIndices[index] - 1
						if rf.hasLogAt(args.PrevLogIndex) {
							args.PrevLogTerm = rf.logAt(args.PrevLogIndex).Term
						} else if args.PrevLogIndex == rf.LastIncludedIndex {
							args.PrevLogTerm = rf.LastIncludedTerm
						}
						args.Entries = rf.logsFromIndex(rf.nextIndices[index])
						if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term != rf.CurrentTerm {
							args.Entries = rf.logsInRange(rf.nextIndices[index], rf.commitIndex)
						}

						args.LeaderCommit = rf.commitIndex

						DPrintf(
							"%d %d retry appendEntries index=%v, reply.XIndex=%v, "+
								"reply.XTerm=%v, PrevLogIndex=%v, PrevLogTerm=%v",
							MillisecondsPassed(rf.startTime),
							rf.me,
							index,
							reply.XIndex,
							reply.XTerm,
							args.PrevLogIndex,
							args.PrevLogTerm)
						rf.mu.Unlock()
						time.Sleep(10 * time.Millisecond)
					}
				}

			}(i, args)
		}
		time.Sleep(heartbeatInterval)
	}
}
