package raft

import (
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Debugging
const Debug = true
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
		// NOTE:
		// follower should keep its vote_for to avoid voting twice
		// rf.vote_for = -1
		rf.vote_recv = -1
		rf.counter = rf.GetElectionCount()
	case Candidate:
		rf.vote_for = rf.me
		rf.vote_recv = 1
		rf.counter = rf.GetElectionCount()
	case Leader:
		DPrintf("{Node %v} become leader", rf.me)
		rf.vote_for = rf.me
		rf.vote_recv = -1
		rf.counter = rf.GetHBCounter()
		rf.next_log_idx = len(rf.log)
		// NOTE:
		// initialize follower_match_idx and follower_next_idx
		for i := range rf.follower_next_idx {
			// this match and next may not true ,
			// need to modify by heartbeat
			rf.follower_match_idx[i] = rf.commit_idx
			rf.follower_next_idx[i] = rf.commit_idx + 1
		}
	}
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
func (rf *Raft) GetAppendArgs(i int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:     rf.curr_term,
		LeaderId: rf.me,

		// NOTE:
		// lab3b need add something
		LeaderCommit: rf.commit_idx,
	}
	if rf.follower_next_idx[i] == rf.next_log_idx {
		// heartbeat
		args.Entries = make([]Entry, 0)
		args.PrevLogIdx = rf.GetLastLog().Index
		args.PrevLogTerm = rf.GetLastLog().Term
		DPrintf("{Node %v} term %v state %v i %v PrevLogTerm %v PrevLogIdx %v", rf.me, rf.curr_term, rf.state, i, args.PrevLogTerm, args.PrevLogIdx)
	} else {
		// log duplication
		n := rf.next_log_idx - rf.follower_match_idx[i] - 1
		args.Entries = make([]Entry, n)
		copy(args.Entries, rf.log[rf.follower_match_idx[i]+1:])
		args.PrevLogIdx = rf.follower_match_idx[i]
		args.PrevLogTerm = rf.log[rf.follower_match_idx[i]].Term
		DPrintf("{Node %v} term %v state %v i %v PrevLogTerm %v PrevLogIdx %v length %v", rf.me, rf.curr_term, rf.state, i, args.PrevLogTerm, args.PrevLogIdx, len(args.Entries))
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
						rf.vote_for = -1
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

func (rf *Raft) GetLastLog() Entry {
	return rf.log[len(rf.log)-1]
}
func (rf *Raft) BroadCastHB() {
	args := make([]*AppendEntriesArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args[i] = rf.GetAppendArgs(i)
		go func(i int) {
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(i, args[i], reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} processing hbreply %v term %v state %v reply %v", rf.me, i, rf.curr_term, rf.state, *reply)
				if rf.curr_term == args[i].Term {
					if args[i].PrevLogIdx == 0 {
						assert(args[i].PrevLogTerm == -1)
						assert(reply.Success)
						assert(rf.state == Leader)
					}
					if rf.state == Leader && reply.Success {
						// log duplication success
						if len(args[i].Entries) == 0 {
							return
						}
						rf.follower_match_idx[i] += len(args[i].Entries)
						rf.follower_next_idx[i] += len(args[i].Entries)
						assert(rf.follower_match_idx[i]+1 == rf.follower_next_idx[i])
						// update commit_idx when commit_idx < follower_match_idx
						if rf.commit_idx < rf.follower_match_idx[i] {
							rf.follower_match_idx[rf.me] = len(rf.log) - 1
							temp := make([]int, len(rf.follower_match_idx))
							copy(temp, rf.follower_match_idx)
							sort.Ints(temp)
							if temp[rf.num_most-1] > rf.commit_idx {
								for _, item := range rf.log[rf.commit_idx : temp[rf.num_most-1]+1] {
									DPrintf("commit %v", item)
								}
								rf.commit_idx = temp[rf.num_most-1]
								rf.cond_var.Signal()
								DPrintf("{Node %v} term %v commit", rf.me, rf.curr_term)
							}
							DPrintf("{Node %v} term %v ,now_commit %v ,target %v", rf.me, rf.curr_term, rf.commit_idx, temp[rf.num_most-1])
						}
						//					if !reply.Success {
						//						if reply.Term > rf.curr_term {
						//							// bigger term from other
						//							assert(!reply.Success)
						//							rf.curr_term = reply.Term
						//							rf.ChangeState(Follower)
						//						} else {
						//							// NOTE:
						//							// log duplication in lab3b
						//							// every hb copy 1 log
						//							// if needed (rpc Successfully)
						//							assert(rf.curr_term == reply.Term)
						//						}
						//					}
					} else if !reply.Success {
						DPrintf("{Node %v} term %v state %v hb %v fail ", rf.me, rf.curr_term, rf.state, i)
						assert(rf.state == Leader)
						if rf.curr_term < reply.Term {
							// this leader is too late
							DPrintf("{Node %v} term %v state %v reply.term is bigger %v from %v ", rf.me, rf.curr_term, rf.state, reply.Term, i)
							rf.curr_term = reply.Term
							rf.ChangeState(Follower)
							rf.vote_for = -1
						} else {
							// follower missing some log
							assert(rf.curr_term == reply.Term)
							// TODO:
							// send prevlog
							rf.follower_match_idx[i] = Max(rf.follower_match_idx[i]-10, 0)
							rf.follower_next_idx[i] = rf.follower_match_idx[i] + 1
							assert(rf.follower_match_idx[i] >= 0)
						}
					}
				} else {
					DPrintf("{Node %v}  term %v state %v args.term %v != rf.term %v", rf.me, rf.curr_term, rf.state, args[i].Term, rf.state)
				}
			} else {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v}  term %v state %v rpc fail %v", rf.me, rf.curr_term, rf.state, i)
			}
		}(i)
	}
}
func (rf *Raft) ApplyRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.cond_var.Wait()
		assert(rf.last_applied < rf.commit_idx)
		for {
			if rf.last_applied == rf.commit_idx {
				break
			}
			rf.last_applied += 1
			assert(rf.log[rf.last_applied].Index == rf.last_applied)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.last_applied].Cmd,
				CommandIndex: rf.last_applied,
			}
			rf.appl_ch <- msg
			DPrintf("{Node %v} term %v apply %v log , msg %v", rf.me, rf.curr_term, rf.last_applied, msg)
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) LogDuplicate(idx int, entries []Entry) {
	if len(entries) == 0 {
		return
	}
	assert(len(rf.log) > idx)
	// NOTE:
	//  minus 2 is to get the NEXT log of PREV log idx
	n := len(rf.log) - idx - 2
	i := 0
	DPrintf("previdx %v loglength %v entrieslen %v n %v", idx, len(rf.log), len(entries), n)
	for ; i <= n && i < len(entries); i++ {
		rf.log[idx+i+1] = entries[i]
	}
	if i < len(entries) {
		rf.log = append(rf.log, entries[i:]...)
	}
	rf.log = rf.log[:idx+len(entries)+1]
}
