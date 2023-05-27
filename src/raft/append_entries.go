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

	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("%d %d receives AppendEntries: term= %v args=%+v", MillisecondsPassed(rf.startTime), rf.me, rf.CurrentTerm, args)

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
		rf.heartbeatCh <- struct{}{}
	}()
	if args.PrevLogIndex > 0 {
		if len(rf.Logs) < args.PrevLogIndex {
			reply.XLen = len(rf.Logs)
			DPrintf("%d %d logs too short XLen=%v", MillisecondsPassed(rf.startTime),
				rf.me,
				reply.XLen)
			return
		} else if args.PrevLogTerm != rf.Logs[args.PrevLogIndex-1].Term {
			reply.XTerm = rf.Logs[args.PrevLogIndex-1].Term
			i := args.PrevLogIndex
			for i > 0 && rf.Logs[i-1].Term == args.PrevLogTerm {
				i--
			}
			reply.XIndex = i
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
		if len(rf.Logs) >= logIndex && rf.Logs[logIndex-1].Term != entry.Term {
			rf.Logs = rf.Logs[:logIndex-1]
			break
		}
	}

	for i := range args.Entries {
		logIndex := i + args.PrevLogIndex + 1
		if logIndex > len(rf.Logs) {
			rf.Logs = append(rf.Logs, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.Logs))
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
			if isFirstHeartbeat || (len(rf.Logs) > 0 && rf.Logs[len(rf.Logs)-1].Term != rf.CurrentTerm) {
				isFirstHeartbeat = false
				args.PrevLogIndex = rf.commitIndex
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.Logs[args.PrevLogIndex-1].Term
				}
			} else if len(rf.Logs) >= rf.nextIndices[i] {
				args.PrevLogIndex = rf.nextIndices[i] - 1
				if args.PrevLogIndex != 0 {
					args.PrevLogTerm = rf.Logs[args.PrevLogIndex-1].Term
				}
				args.Entries = rf.Logs[args.PrevLogIndex:]
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
							if ((count << 1) > len(rf.peers)) && (rf.Logs[i-1].Term == rf.CurrentTerm) {
								rf.commitIndex = i
							}
						}
						if origCommitIndex != rf.commitIndex {
							DPrintf("%d %d commitIndex=%v", MillisecondsPassed(rf.startTime), rf.me, rf.commitIndex)
						}

						rf.mu.Unlock()
						break
					} else {
						//rf.nextIndices[index] = min(rf.nextIndices[index], 1)
						//args.PrevLogIndex = rf.nextIndices[index] - 1
						//if args.PrevLogIndex > 0 {
						//	args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
						//}
						//
						//i := len(rf.Logs)
						//for i > 0 && rf.Logs[i-1].Term != rf.CurrentTerm && rf.commitIndex < i {
						//	i--
						//}
						//args.Entries = rf.Logs[args.PrevLogIndex:i]

						firstEntryWithTerm := 0
						for i := len(rf.Logs) - 1; i >= 0 && rf.Logs[i].Term >= reply.XTerm; i-- {
							if rf.Logs[i].Term == reply.XTerm {
								firstEntryWithTerm = i + 1
							}
						}

						if reply.XTerm != 0 && firstEntryWithTerm == 0 {
							rf.nextIndices[index] = reply.XIndex
						} else if firstEntryWithTerm > 0 {
							rf.nextIndices[index] = firstEntryWithTerm
						} else {
							rf.nextIndices[index] = max(reply.XLen, 1)
						}
						rf.nextIndices[index] = max(rf.nextIndices[index], 1)
						args.PrevLogIndex = rf.nextIndices[index] - 1

						j := len(rf.Logs)
						for j > rf.commitIndex && rf.Logs[j-1].Term != rf.CurrentTerm {
							j--
						}
						args.Entries = rf.Logs[args.PrevLogIndex:j]

						rf.mu.Unlock()
						time.Sleep(10 * time.Millisecond)
					}
				}

			}(i, args)
		}
		time.Sleep(heartbeatInterval)
	}
}
