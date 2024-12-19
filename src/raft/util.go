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
const ElectionTimeout = 10
const HBTimeout = 6

var rnd = rand.New(rand.NewSource(time.Now().Unix()))
var rnd_mtx sync.Mutex

// big for election
func (rf *Raft) GetBigCounter() int {
	rnd_mtx.Lock()
	defer rnd_mtx.Unlock()
	return rnd.Intn(ElectionTimeout) + ElectionTimeout
}

// small for heartbeat
func (rf *Raft) GetSmallCounter() int {
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
		rf.counter = rf.GetBigCounter()
	case Candidate:
		rf.Vote_for = rf.me
		rf.vote_recv = 1
		rf.counter = rf.GetBigCounter()
	case Leader:
		DPrintf("{Node %v} become leader", rf.me)
		rf.Vote_for = rf.me
		rf.vote_recv = -1
		rf.counter = rf.GetSmallCounter()
		rf.next_log_idx = rf.GetLastLog().Index + 1
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
func (rf *Raft) GetAppendArgs(i int) HBArgs {
	ret := HBArgs{
		append_args:   nil,
		snapshot_args: nil,
	}
	args := &AppendEntriesArgs{
		Term:     rf.Curr_term,
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
		DPrintf("{Node %v} term %v state %v i %v PrevLogTerm %v PrevLogIdx %v lastlog %v", rf.me, rf.Curr_term, rf.state, i, args.PrevLogTerm, args.PrevLogIdx, rf.GetLastLog())
		ret.append_args = args
	} else if rf.GetLastLog().Term != rf.Curr_term {
		// NOTE:figure 8
		// leader only commit log whose term == Curr_term
		DPrintf("{Node %v} term %v state %v i %v LastLogTerm %v Curr_term %v", rf.me, rf.Curr_term, rf.state, i, rf.GetLastLog().Term, rf.Curr_term)
		assert(rf.GetLastLog().Term <= rf.Curr_term)
		args.Entries = make([]Entry, 0)
		args.PrevLogIdx = rf.GetLastLog().Index
		args.PrevLogTerm = rf.GetLastLog().Term
		ret.append_args = args
	} else {
		// log duplication
		if rf.snapshot_idx <= rf.follower_match_idx[i] {
			n := rf.GetLastLog().Index - rf.follower_match_idx[i]
			DPrintf("{Node %v} term %v state %v i %v rf.next_log_idx %v follower_match_idx[i] %v", rf.me, rf.Curr_term, rf.state, i, rf.next_log_idx, rf.follower_match_idx[i])
			args.Entries = make([]Entry, n)
			// there should subtract snapshot_idx
			copy(args.Entries, rf.Log[rf.GetRealIdx(rf.follower_match_idx[i])+1:])
			args.PrevLogIdx = rf.follower_match_idx[i]
			args.PrevLogTerm = rf.Log[rf.GetRealIdx(rf.follower_match_idx[i])].Term
			DPrintf("{Node %v} term %v state %v i %v PrevLogTerm %v PrevLogIdx %v length %v", rf.me, rf.Curr_term, rf.state, i, args.PrevLogTerm, args.PrevLogIdx, len(args.Entries))
			ret.append_args = args
			ret.snapshot_args = nil
		} else {
			// no log to send to peer i
			// need send sanpshot
			DPrintf("{Node %v} term %v state %v i %v snapshot_idx %v prev_snapshot_idx %v length %v", rf.me, rf.Curr_term, rf.state, i, rf.snapshot_idx, rf.prev_snapshot_idx, len(args.Entries))
			assert(rf.snapshot_term == rf.Log[rf.snapshot_idx-rf.prev_snapshot_idx].Term)
			snapshot_args := InstallSnapshotArgs{
				Term:            rf.Curr_term,
				LeaderId:        rf.me,
				LastIncludeIdx:  rf.snapshot_idx,
				LastIncludeTerm: rf.snapshot_term,
				Snapshot:        rf.SnapShotData,
			}
			ret.append_args = nil
			ret.snapshot_args = &snapshot_args
		}
	}
	assert((ret.append_args == nil && ret.snapshot_args != nil) || (ret.append_args != nil && ret.snapshot_args == nil))
	return ret
}

func (rf *Raft) GetVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:        rf.Curr_term,
		CandidateId: rf.me,
		// NOTE:
		// lab3b:
		// lastlog
		// NOT LASTCOMMITLOG
		LastLogIndex: rf.GetLastLog().Index,
		LastLogTerm:  rf.GetLastLog().Term,
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
				if args.Term == rf.Curr_term && rf.state == Candidate {
					if reply.Term > rf.Curr_term {
						// bigger term from other
						rf.Curr_term = reply.Term
						rf.ChangeState(Follower)
						rf.Vote_for = -1
						rf.persist()
					} else if reply.VoteGranted {
						// get vote
						rf.vote_recv += 1
						DPrintf("{Node %v} recv vote from %v , now vote_recv %v ,now term %v ,arg_term %v", rf.me, i, rf.vote_recv, rf.Curr_term, args.Term)
						if rf.vote_recv >= (len(rf.peers)+1)/2 {
							rf.ChangeState(Leader)
							rf.counter = rf.GetSmallCounter()
							rf.BroadCastHB()
							// rf.persist()
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
	return rf.Log[len(rf.Log)-1]
}

func (rf *Raft) GetRealLogLen() int {
	DPrintf("{Node %v} GetLastLog().index + 1 %v len(log)+prev_snapshot_idx %v", rf.me, rf.GetLastLog().Index+1, len(rf.Log)+rf.prev_snapshot_idx)
	assert(rf.GetLastLog().Index+1 == len(rf.Log)+rf.prev_snapshot_idx)
	return rf.GetLastLog().Index + 1
}

func (rf *Raft) BroadCastHB() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		arg := rf.GetAppendArgs(i)
		go func(i int, arg HBArgs) {
			reply := &AppendEntriesReply{}
			if arg.append_args != nil {
				if rf.sendAppendEntries(i, arg.append_args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("{Node %v} processing hbreply %v term %v state %v reply %v len(args) %v", rf.me, i, rf.Curr_term, rf.state, *reply, len(arg.append_args.Entries))
					if rf.Curr_term == arg.append_args.Term {
						assert(rf.state == Leader)
						if reply.Success {
							// log duplication success
							//if len(arg.append_args.Entries) == 0 {
							//	rf.follower_match_idx[i] = arg.append_args.PrevLogIdx
							//	rf.follower_next_idx[i] = rf.follower_match_idx[i] + 1
							//	return
							//}
							// NOTE:
							// use prevlogidx + len not +=
							// because there is a case:
							// when a rpc send and block on lock()
							// fortunately next heartbeat args are got
							// then if rpc success , follower_match_idx will += twice.
							rf.follower_match_idx[i] = arg.append_args.PrevLogIdx + len(arg.append_args.Entries)
							rf.follower_next_idx[i] = rf.follower_match_idx[i] + 1
							// update commit_idx when commit_idx < follower_match_idx
							if rf.commit_idx < rf.follower_match_idx[i] {
								rf.follower_match_idx[rf.me] = rf.GetRealLogLen() - 1
								temp := make([]int, len(rf.follower_match_idx))

								copy(temp, rf.follower_match_idx)
								sort.Ints(temp)
								if temp[rf.num_most-1] > rf.commit_idx {
									for _, item := range rf.Log[rf.GetRealIdx(rf.commit_idx+1):rf.GetRealIdx(temp[rf.num_most-1]+1)] {
										DPrintf("commit %v", item)
									}
									rf.commit_idx = temp[rf.num_most-1]
									rf.cond_var.Signal()
									DPrintf("{Node %v} term %v commit", rf.me, rf.Curr_term)
								}
								DPrintf("{Node %v} term %v ,now_commit %v ,target %v", rf.me, rf.Curr_term, rf.commit_idx, temp[rf.num_most-1])
							}
						} else if !reply.Success {
							DPrintf("{Node %v} term %v state %v hb %v fail ", rf.me, rf.Curr_term, rf.state, i)
							if rf.Curr_term < reply.Term {
								// this leader is too late
								DPrintf("{Node %v} term %v state %v reply.term is bigger %v from %v ", rf.me, rf.Curr_term, rf.state, reply.Term, i)
								rf.Curr_term = reply.Term
								rf.ChangeState(Follower)
								rf.Vote_for = -1
							} else {
								// follower missing some log
								assert(rf.Curr_term == reply.Term)
								rf.follower_match_idx[i] = Max(rf.snapshot_idx-1, rf.follower_match_idx[i]-100)
								rf.follower_next_idx[i] = rf.follower_match_idx[i] + 1
							}
						}
					}
				}
			} else {
				assert(arg.snapshot_args != nil)
				snapshot_reply := &InstallSnapshotReply{}
				DPrintf("{Node %v} sendInstallSnapshot %v term %v state %v snapshot_idx %v", rf.me, i, rf.Curr_term, rf.state, rf.snapshot_idx)
				if rf.sendInstallSnapshot(i, arg.snapshot_args, snapshot_reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if arg.snapshot_args.Term == rf.Curr_term {
						// 1. rpc reply must be in the same term as args
						assert(rf.state == Leader)
						if snapshot_reply.Success {
							// if reply success
							// snapshot copy
							assert(snapshot_reply.Term <= rf.Curr_term)
							assert(rf.snapshot_idx >= arg.snapshot_args.LastIncludeIdx)
							rf.follower_match_idx[i] = arg.snapshot_args.LastIncludeIdx
							rf.follower_next_idx[i] = rf.follower_match_idx[i] + 1
							assert(rf.follower_match_idx[i] >= 0)
						} else {
							// if reply fail
							// only 1 case that is leader term is not bigger than this peer
							assert(snapshot_reply.Term > rf.Curr_term)
							rf.ChangeState(Follower)
							rf.Curr_term = snapshot_reply.Term
						}
					}
				}

			}
		}(i, arg)
	}
}
func (rf *Raft) ApplyRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		assert(rf.last_applied <= rf.commit_idx)
		for rf.last_applied == rf.commit_idx && (rf.fromTop == true || rf.prev_snapshot_idx == rf.snapshot_idx) && rf.force_apply == false {
			DPrintf("{Node %v} wait", rf.me)
			rf.cond_var.Wait()
		}
		// NOTE:
		// apply snapshot first
		DPrintf("{Node %v} awake", rf.me)
		if rf.snapshot_idx != rf.prev_snapshot_idx || rf.force_apply {
			rf.force_apply = false
			msg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				SnapshotIndex: rf.snapshot_idx,
				SnapshotTerm:  rf.snapshot_term,
				Snapshot:      rf.SnapShotData,
			}
			if rf.snapshot_idx <= rf.GetLastLog().Index {
				assert(rf.snapshot_idx == rf.Log[rf.snapshot_idx-rf.prev_snapshot_idx].Index)
			}
			DPrintf("{Node %v} apply snapshot term %v snapshot_idx %v", rf.me, rf.Curr_term, rf.snapshot_idx)
			rf.mu.Unlock()
			rf.appl_ch <- msg
			rf.mu.Lock()
			// after InstallSnapshot
			// it is need to apply the following logs after snapshot_idx
			if rf.last_applied < msg.SnapshotIndex {
				rf.last_applied = msg.SnapshotIndex
			}
			if msg.SnapshotIndex > rf.GetLastLog().Index {
				// if snapshot has included all log
				rf.Log = make([]Entry, 1)
				rf.Log[0].Index = msg.SnapshotIndex
				rf.Log[0].Term = msg.SnapshotTerm
			} else if rf.prev_snapshot_idx != msg.SnapshotIndex {
				// NOTE:
				// this case is that
				// when a node restart, it need to re-apply snapshot although it prev_snapshot_idx == msg.snapshot_idx.
				// so only if prev_snapshot_idx != snapshot_idx ,it need to compact our logs
				rf.Log = rf.Log[msg.SnapshotIndex-rf.prev_snapshot_idx:]
				assert(rf.Log[0].Index == msg.SnapshotIndex)
				rf.Log[0].Term = msg.SnapshotTerm
			}
			if msg.SnapshotIndex > rf.commit_idx {
				// NOTE:
				// InstallSnapshot can be used as hb
				// so if snapshot_idx > commit_idx
				// we can think that logs whose idx < snapshot_idx were committed
				rf.commit_idx = msg.SnapshotIndex
			}

			rf.prev_snapshot_idx = msg.SnapshotIndex
			DPrintf("{Node %v} install snapshot to statemachine term %v snapshot_idx %v last_applied %v log %v", rf.me, rf.Curr_term, rf.snapshot_idx, rf.last_applied, rf.Log)
		}
		rf.persist()

		// apply logs
		assert(rf.last_applied <= rf.commit_idx)
		if rf.last_applied == rf.commit_idx {
			// if there is no log to apply
			// cause snapshot apply delete all obselete
			assert(len(rf.Log) >= 1)
			rf.mu.Unlock()
			continue
		}
		temp_last_applied := rf.last_applied
		msgs := make([]ApplyMsg, 0, rf.commit_idx-rf.last_applied)
		for {
			if temp_last_applied == rf.commit_idx {
				break
			}
			temp_last_applied += 1
			assert(rf.Log[rf.GetRealIdx(temp_last_applied)].Index == temp_last_applied)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.GetRealIdx(temp_last_applied)].Cmd,
				CommandIndex: temp_last_applied,
			}
			msgs = append(msgs, msg)
			DPrintf("{Node %v} apply log term %v apply %v commit_idx %v, msg %v", rf.me, rf.Curr_term, rf.last_applied, rf.commit_idx, msg)
		}
		assert(len(msgs) == rf.commit_idx-rf.last_applied)
		rf.mu.Unlock()
		// NOTE:
		// why prepare all msgs ,then send them all ?
		// because tester code will call snapshot every time we apply some logs
		// peers which appling logs need to hold lock, but snapshot also need to hold lock.
		// So log-appling cannot hold lock.
		// We prepare messages first, then send them all without holding lock
		for _, msg := range msgs {
			rf.appl_ch <- msg
		}
		rf.mu.Lock()
		rf.last_applied += len(msgs)
		rf.mu.Unlock()

	}
}
func (rf *Raft) LogDuplicate(idx int, entries []Entry) {
	if len(entries) == 0 {
		return
	}
	assert(rf.GetRealLogLen() > idx)
	// NOTE:
	//  minus 2 is to get the NEXT log of PREV log idx
	n := rf.GetRealLogLen() - idx - 2
	i := 0
	DPrintf("{Node %v} previdx %v loglength %v entrieslen %v n %v", rf.me, idx, rf.GetRealLogLen(), len(entries), n)
	for ; i <= n && i < len(entries); i++ {
		rf.Log[rf.GetRealIdx(idx+i+1)] = entries[i]
	}
	if i < len(entries) {
		rf.Log = append(rf.Log, entries[i:]...)
	}
	rf.Log = rf.Log[:rf.GetRealIdx(idx+len(entries)+1)]
	DPrintf("{Node %v} after copy log loglength %v", rf.me, rf.GetRealLogLen())
	rf.persist()
}
func (rf *Raft) GetRealIdx(idx int) int {
	if idx-rf.prev_snapshot_idx != len(rf.Log) {
		DPrintf("{Node %v} GetRealIdx loglen %v idx %v prev_snapshot_idx %v log %v", rf.me, len(rf.Log), idx, rf.prev_snapshot_idx, rf.Log)
		assert(rf.Log[idx-rf.prev_snapshot_idx].Index == idx)
	}
	return idx - rf.prev_snapshot_idx
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Curr_term > args.Term {
		reply.Success = false
		reply.Term = rf.Curr_term
		return
	}
	rf.Curr_term = args.Term
	rf.ChangeState(Follower)
	rf.counter = rf.GetBigCounter()
	reply.Success = true
	rf.SnapShotData = args.Snapshot
	rf.snapshot_term = args.LastIncludeTerm
	// NOTE:
	// we should not cut log in this func
	// log cutting should be did in applier
	// idx := args.LastIncludeIdx
	// if idx > rf.GetLastLog().Index {
	// 	rf.Log = make([]Entry, 1)
	// 	rf.Log[0].Index = idx
	// 	rf.Log[0].Term = args.LastIncludeTerm
	// } else {
	// 	rf.Log = rf.Log[idx-rf.snapshot_idx:]
	// }
	rf.snapshot_idx = args.LastIncludeIdx
	rf.persist()
	rf.fromTop = false
	DPrintf("{Node %v} recvinstallsnapshot term %v state %v snapshot_idx %v firstlogidx %v", rf.me, rf.Curr_term, rf.state, rf.snapshot_idx, rf.Log[0].Index)
	rf.cond_var.Signal()

}
