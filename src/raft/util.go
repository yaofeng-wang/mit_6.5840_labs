package raft

import (
	"log"
	"time"
)

// Debug Debugging
const Debug = true

func init() {
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func MillisecondsPassed(start time.Time) int {
	return int(time.Since(start).Milliseconds())
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
