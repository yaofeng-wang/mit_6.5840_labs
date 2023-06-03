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
	// will return LastIncludedIndex if there is no logs, should always check if log isEmpty first.
	return rf.LastIncludedIndex + len(rf.Logs)
}

func (rf *Raft) indexBeforeLastLog() int {
	if rf.isEmpty() {
		return rf.lastLogIndex()
	}
	return rf.lastLogIndex() - 1
}

func (rf *Raft) appendLogs(logEntry ...logEntry) {
	rf.Logs = append(rf.Logs, logEntry...)
}

func (rf *Raft) deleteLogsFromIndex(logIndex int) {
	// delete log at logIndex as well
	rf.Logs = rf.Logs[:logIndex-rf.LastIncludedIndex-1]
}

func (rf *Raft) getFirstIndexWithTerm(term int) int {
	firstIndex := rf.LastIncludedIndex + 1
	for index := rf.lastLogIndex(); rf.hasLogAt(index); index-- {
		if rf.logAt(index).Term == term {
			firstIndex = index
		}
	}
	return firstIndex
}

func (rf *Raft) logsFromIndex(logIndex int) Logs {
	return rf.Logs[logIndex-rf.LastIncludedIndex-1:]
}

func (rf *Raft) logsInRange(from int, to int) Logs {
	var entries Logs
	for i := from; i <= to; i++ {
		if rf.hasLogAt(i) {
			entries = append(entries, *rf.logAt(i))
		}
	}
	return entries
}
