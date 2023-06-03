package raft

type (
	logEntry struct {
		Term    int
		Command interface{}
	}

	Logs []logEntry
)

func (rf *Raft) hasLogAt(logIndex int) bool {
	if logIndex <= rf.LastIncludedIndex {
		return false
	}
	return logIndex <= rf.LastIncludedIndex+len(rf.Logs)
}

func (rf *Raft) logAt(logIndex int) *logEntry {
	if !rf.hasLogAt(logIndex) {
		return nil
	}
	return &rf.Logs[logIndex-rf.LastIncludedIndex-1]
}

func (rf *Raft) length() int {
	return len(rf.Logs)
}

func (rf *Raft) isEmpty() bool {
	return rf.length() == 0
}

func (rf *Raft) lastLog() *logEntry {
	if rf.isEmpty() {
		return nil
	}
	return rf.logAt(rf.length())
}

func (rf *Raft) lastLogIndex() int {
	return rf.LastIncludedIndex + len(rf.Logs)
}

func (rf *Raft) appendLogs(logEntry ...logEntry) {
	rf.Logs = append(rf.Logs, logEntry...)
}
