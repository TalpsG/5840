package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) GetVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:        rf.curr_term,
		CandidateId: rf.me,
		// lab3b:
		// NOTE:
		// lastlog
		// NOT LASTCOMMITLOG
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	return args
}
