package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = false
const ElectionTimeout = 15
const HBTimeout = 5

var rnd = rand.New(rand.NewSource(time.Now().Unix()))
var rnd_mtx sync.Mutex

func (rf *Raft) GetElectionCount() int {
	rnd_mtx.Lock()
	defer rnd_mtx.Unlock()
	return rnd.Intn(ElectionTimeout) + ElectionTimeout
}
func (rf *Raft) GetHBCounter() int {
	rnd_mtx.Lock()
	defer rnd_mtx.Unlock()
	return rnd.Intn(HBTimeout) + HBTimeout
}

func (rf *Raft) ChangeState(s State) {
	DPrintf("{Node %v} changes state from %v to %v , vote_recv %v ", rf.me, rf.state, s, rf.vote_recv)
	rf.state = s
	switch s {
	case Follower:
		rf.vote_for = -1
		rf.vote_recv = -1
		rf.counter = rf.GetElectionCount()
	case Candidate:
		rf.vote_for = rf.me
		rf.vote_recv = 1
		rf.counter = rf.GetElectionCount()
	case Leader:
		rf.vote_for = rf.me
		rf.vote_recv = -1
		rf.counter = rf.GetHBCounter()
	}
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
func (rf *Raft) GetAppendArgs() *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:     rf.curr_term,
		LeaderId: rf.me,

		// NOTE:
		// lab3b need add something
	}
	return args
}

func (rf *Raft) GetVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:        rf.curr_term,
		CandidateId: rf.me,
		// NOTE:
		// lab3b:
		// lastlog
		// NOT LASTCOMMITLOG
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	return args
}
func (rf *Raft) StartElect() {
	args := rf.GetVoteArgs()
	DPrintf("{Node %v} starts election with RequestVoteArgs %v recv_vote %v", rf.me, args, rf.vote_recv)
	assert(rf.vote_recv == 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(i, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if args.Term == rf.curr_term && rf.state == Candidate {
					if reply.Term > rf.curr_term {
						// bigger term from other
						rf.curr_term = reply.Term
						rf.ChangeState(Follower)
					} else if reply.VoteGranted {
						// get vote
						rf.vote_recv += 1
						DPrintf("{Node %v} recv vote from %v , now vote_recv %v ,now term %v ,arg_term %v", rf.me, i, rf.vote_recv, rf.curr_term, args.Term)
						if rf.vote_recv >= (len(rf.peers)+1)/2 {
							rf.ChangeState(Leader)
							rf.counter = rf.GetHBCounter()
							rf.BroadCastHB()
						}
					}
				}

			}

		}(i)

	}

}
func assert(t bool) {
	if !t {
		panic("bool ")
	}
}
func (rf *Raft) BroadCastHB() {
	args := rf.GetAppendArgs()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(i, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.curr_term == args.Term && rf.state == Leader {
					if !reply.Success {
						if reply.Term > rf.curr_term {
							// bigger term from other
							assert(!reply.Success)
							rf.curr_term = reply.Term
							rf.ChangeState(Follower)
						} else {
							// NOTE:
							// log duplication in lab3b
							assert(rf.curr_term == reply.Term)
						}
					}
				}
			}
		}(i)
	}
}
