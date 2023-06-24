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

	XTerm, XIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("%d %d receives AppendEntries: term=%v args=%+v rf.lastLogIndex()=%v rf.commitIndex=%v rf.lastApplied=%v",
		MillisecondsPassed(rf.startTime), rf.me, rf.CurrentTerm, args, rf.lastLogIndex(), rf.commitIndex, rf.lastApplied)
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
			reply.XIndex = rf.lastLogIndex()
			if rf.hasLogAt(rf.lastLogIndex()) {
				reply.XTerm = rf.logAt(rf.lastLogIndex()).Term
			} else {
				reply.XTerm = rf.LastIncludedTerm
			}
			DPrintf("%d %d logs too short rf.lastLogIndex()=%v args.PrevLogIndex=%v", MillisecondsPassed(rf.startTime),
				rf.me, rf.lastLogIndex(), args.PrevLogIndex)
			return
		} else if rf.hasLogAt(args.PrevLogIndex) && args.PrevLogTerm != rf.logAt(args.PrevLogIndex).Term {
			reply.XIndex = args.PrevLogIndex - 1
			reply.XTerm = min(args.PrevLogTerm, rf.logAt(args.PrevLogIndex).Term)

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
		if rf.commitIndex == 0 {
			DPrintf("%d %d rf.commitIndex == 0", MillisecondsPassed(rf.startTime), rf.me)
		}
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
					DPrintf("%d %d [error]PrevLogIndex is in snapshot i=%v args.PrevLogIndex=%v rf.LastIncludedIndex=%v",
						MillisecondsPassed(rf.startTime), rf.me, i, args.PrevLogIndex, rf.LastIncludedIndex)
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
					go func(index int) {
						rf.mu.Lock()
						args := &InstallSnapshotArgs{
							Term:              rf.CurrentTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.LastIncludedIndex,
							LastIncludedTerm:  rf.LastIncludedTerm,
							Data:              rf.SnapshotLogs,
						}
						rf.mu.Unlock()
						for ii := 0; ii < 9; ii++ {
							reply := &InstallSnapshotReply{}
							DPrintf("%d %d PrevLogIndex is in snapshot rf.nextIndices[%v]=%v rf.LastIncludedIndex=%v", MillisecondsPassed(rf.startTime), rf.me, index, rf.nextIndices[index], rf.LastIncludedIndex)
							if ok := rf.sendInstallSnapshot(i, args, reply); ok {
								rf.mu.Lock()
								if reply.Term > rf.CurrentTerm {
									rf.state = follower
									rf.CurrentTerm = reply.Term
									rf.VotedFor = nil
									rf.persist()
								}

								rf.matchIndices[index] = max(rf.matchIndices[index], rf.LastIncludedIndex)
								rf.nextIndices[index] = max(rf.nextIndices[index], rf.LastIncludedIndex+1)
								rf.mu.Unlock()
								return
							}
						}
					}(i)
					rf.mu.Unlock()
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

					rf.mu.Unlock()
					if reply.Success {
						rf.mu.Lock()
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
								if rf.commitIndex == 0 {
									DPrintf("%d %d rf.commitIndex == 0", MillisecondsPassed(rf.startTime), rf.me)
								}
							}
						}
						if origCommitIndex != rf.commitIndex {
							DPrintf("%d %d commitIndex=%v", MillisecondsPassed(rf.startTime), rf.me, rf.commitIndex)
						}
						rf.mu.Unlock()
						return
					}
					rf.mu.Lock()
					DPrintf("%d %d appendEntries failure received index=%v reply.XIndex=%v rf.LastIncludedIndex=%v",
						MillisecondsPassed(rf.startTime), rf.me, index, reply.XIndex, rf.LastIncludedIndex)
					rf.nextIndices[index] = reply.XIndex
					if reply.XIndex <= rf.LastIncludedIndex {
						DPrintf("%d %d reply.XIndex < rf.LastIncludedIndex index=%v reply.XIndex=%v rf.LastIncludedIndex=%v",
							MillisecondsPassed(rf.startTime), rf.me, index, reply.XIndex, rf.LastIncludedIndex)
						args := &InstallSnapshotArgs{
							Term:              rf.CurrentTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.LastIncludedIndex,
							LastIncludedTerm:  rf.LastIncludedTerm,
							Data:              rf.SnapshotLogs,
						}
						rf.mu.Unlock()
						for ii := 0; ii < 9; ii++ {
							DPrintf("%d %d PrevLogIndex in %v is in snapshot reply.XIndex =%v rf.LastIncludedIndex=%v", MillisecondsPassed(rf.startTime), rf.me, index, reply.XIndex, rf.LastIncludedIndex)
							reply := &InstallSnapshotReply{}
							if ok := rf.sendInstallSnapshot(index, args, reply); ok {
								rf.mu.Lock()
								if reply.Term > rf.CurrentTerm {
									rf.state = follower
									rf.CurrentTerm = reply.Term
									rf.VotedFor = nil
									rf.persist()
								}

								rf.matchIndices[index] = max(rf.matchIndices[index], rf.LastIncludedIndex)
								rf.nextIndices[index] = max(rf.nextIndices[index], rf.LastIncludedIndex+1)
								rf.mu.Unlock()
								return
							}
						}
					} else {
						lastEntryWithSmallerTerm := rf.LastIncludedIndex + 1
						for i := lastEntryWithSmallerTerm; rf.hasLogAt(i) && rf.logAt(i).Term < reply.XTerm && i <= reply.XIndex; i++ {
							lastEntryWithSmallerTerm = i
						}
						if rf.hasLogAt(lastEntryWithSmallerTerm) {
							DPrintf("%d %d retry with smaller term index=%v reply.XIndex=%v rf.LastIncludedIndex=%v "+
								"rf.commitIndex=%v reply.XTerm=%v rf.logAt(lastEntryWithSmallerTerm).Term=%v "+
								"lastEntryWithSmallerTerm=%v",
								MillisecondsPassed(rf.startTime), rf.me, index, reply.XIndex, rf.LastIncludedIndex, rf.commitIndex, reply.XTerm, rf.logAt(lastEntryWithSmallerTerm).Term,
								lastEntryWithSmallerTerm)
						}

						rf.nextIndices[index] = lastEntryWithSmallerTerm
						args.PrevLogIndex = rf.nextIndices[index] - 1
						if rf.hasLogAt(args.PrevLogIndex) {
							args.PrevLogTerm = rf.logAt(args.PrevLogIndex).Term
							args.Entries = rf.logsFromIndex(rf.nextIndices[index])
						} else if args.PrevLogIndex == rf.LastIncludedIndex {
							args.PrevLogTerm = rf.LastIncludedTerm
							args.Entries = rf.logsFromIndex(rf.nextIndices[index])
						} else {
							DPrintf("%d %d [error] unexpected args.PrevLogIndex rf.nextIndices[%v]=%v rf.LastIncludedIndex=%v", MillisecondsPassed(rf.startTime), rf.me, index, rf.nextIndices[index], rf.LastIncludedIndex)
						}

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

					}
					time.Sleep(10 * time.Millisecond)

				}

			}(i, args)
		}
		time.Sleep(heartbeatInterval)
	}
}
