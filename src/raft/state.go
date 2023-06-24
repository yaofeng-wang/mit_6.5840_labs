package raft

type State int

const (
	follower State = iota
	candidate
	leader
)
