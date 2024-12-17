package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Cmd   interface{}
	Term  int
	Index int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state     State
	Curr_term int
	Vote_for  int
	Log       []Entry

	// volatile state
	commit_idx   int
	last_applied int

	// volatile on leader
	follower_next_idx  []int
	follower_match_idx []int

	vote_recv int
	// counter
	counter int

	// next log idx
	next_log_idx int

	// half +1
	num_most int

	// bgworker apply log to statemachine
	appl_ch  chan ApplyMsg
	cond_var sync.Cond

	// snapshot log idx
	snapshot_idx      int
	prev_snapshot_idx int
	snapshot_term     int
	SnapShotData      []byte
	// is snapshot from Snapshot API or InstallSnapshot
	fromTop     bool
	force_apply bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.Curr_term, rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// NOTE encoding and decoding must be a same sequence
	e.Encode(rf.Curr_term)
	e.Encode(rf.Vote_for)
	// lab3d
	e.Encode(rf.snapshot_idx)
	e.Encode(rf.prev_snapshot_idx)
	e.Encode(rf.snapshot_term)
	e.Encode(rf.Log)
	raftstate := w.Bytes()
	DPrintf("Persist {Node %v} curr_term %v vote_for %v loglength %v", rf.me, rf.Curr_term, rf.Vote_for, rf.Log)
	rf.persister.Save(raftstate, rf.SnapShotData)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var vote int
	var snapshot_idx int
	var prev_snapshot_idx int
	var snapshot_term int
	var log []Entry
	if d.Decode(&term) != nil ||
		d.Decode(&vote) != nil ||
		d.Decode(&snapshot_idx) != nil ||
		d.Decode(&prev_snapshot_idx) != nil ||
		d.Decode(&snapshot_term) != nil ||
		d.Decode(&log) != nil {
		panic("decode type error ")
	}

	rf.Curr_term = term
	rf.Vote_for = vote
	rf.snapshot_idx = snapshot_idx
	rf.prev_snapshot_idx = prev_snapshot_idx
	rf.snapshot_term = snapshot_term
	rf.Log = log
	rf.persister.ReadRaftState()
	rf.SnapShotData = rf.persister.ReadSnapshot()
	if len(rf.SnapShotData) == 0 {
		rf.SnapShotData = nil
	}

	assert(rf.state == Follower)
	rf.state = Follower
	for i := 0; i < len(rf.follower_match_idx); i++ {
		rf.follower_match_idx[i] = 0
		rf.follower_next_idx[i] = 1
	}
	// lab3d
	if rf.snapshot_idx == rf.prev_snapshot_idx {
		rf.last_applied = rf.snapshot_idx
	} else {
		rf.last_applied = rf.prev_snapshot_idx
	}
	rf.commit_idx = rf.last_applied
	if rf.SnapShotData != nil {
		rf.force_apply = true
	}
	DPrintf("readPersist {Node %v} curr_term %v vote_for %v loglength %v", rf.me, rf.Curr_term, rf.Vote_for, rf.Log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

// NOTE:
// snapshot must be called within lock
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	assert(rf.Log[0].Index == rf.snapshot_idx)
	assert(index > rf.Log[0].Index)
	assert(index > rf.snapshot_idx)
	assert(index <= rf.commit_idx)
	rf.snapshot_term = rf.Log[rf.GetRealIdx(index)].Term
	rf.SnapShotData = snapshot
	// NOTE:
	// should not cut log
	rf.Log = rf.Log[index-rf.snapshot_idx:]
	rf.snapshot_idx = index
	rf.prev_snapshot_idx = rf.snapshot_idx
	rf.fromTop = true
	DPrintf("snapshot {Node %v} term %v state %v snapshot_idx %v loglen %v log %v", rf.me, rf.Curr_term, rf.state, rf.snapshot_idx, len(rf.Log), rf.Log)

	rf.persist()
}

type InstallSnapshotArgs struct {
	Term            int
	LeaderId        int
	LastIncludeIdx  int
	LastIncludeTerm int
	Snapshot        []byte
}
type InstallSnapshotReply struct {
	Success bool
	Term    int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type HBArgs struct {
	append_args   *AppendEntriesArgs
	snapshot_args *InstallSnapshotArgs
}

type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing RequestVote,  RequestVoteArgs %v and RequestVoteReply %v , vote_for %v", rf.me, rf.state, rf.Curr_term, args, reply, rf.Vote_for)
	if args.Term < rf.Curr_term || (args.Term == rf.Curr_term && rf.Vote_for != -1 && rf.Vote_for != args.CandidateId) {
		// 1. term 小
		// 2. 已经投过票了
		reply.Term = rf.Curr_term
		reply.VoteGranted = false
		// NOTE save
		rf.persist()
		return
	}
	if args.Term > rf.Curr_term {
		rf.ChangeState(Follower)
		rf.Curr_term = args.Term
		rf.Vote_for = -1
	}

	reply.Term = args.Term
	rf.counter = rf.GetBigCounter()
	DPrintf("{Node %v} term %v ,last term %v ,args.last_term %v lastlogidx %v args.lastlogidx %v", rf.me, rf.Curr_term, rf.GetLastLog().Term, args.LastLogTerm, rf.GetLastLog().Index, args.LastLogIndex)
	if rf.GetLastLog().Term > args.LastLogTerm {
		reply.VoteGranted = false
		if rf.Vote_for != -1 {
			rf.Vote_for = -1
			rf.persist()
		}
		// NOTE:
		// if candidate last log is not newer than me
		// me should be more possible to be leader
		// so let me to start elect quickly (by get a small counter)
		rf.counter = rf.GetSmallCounter()
		DPrintf("{Node %v} term %v lastlogterm not newer", rf.me, rf.Curr_term)
		// NOTE save
		return
	}
	if rf.GetLastLog().Term == args.LastLogTerm && rf.GetLastLog().Index > args.LastLogIndex {
		reply.VoteGranted = false
		if rf.Vote_for != -1 {
			rf.Vote_for = -1
			rf.persist()
		}
		// NOTE:
		// same reason as lastlogterm not newer
		rf.counter = rf.GetSmallCounter()
		DPrintf("{Node %v} term %v lastlog idx not bigger", rf.me, rf.Curr_term)
		// NOTE save
		return

	}
	rf.Vote_for = args.CandidateId
	reply.VoteGranted = true
	// NOTE save
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("recv appendentries %v", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v}'s state is {state %v, term %v}} processing AppendEntries,  AppendEntriesArgs %v and AppendEntriesReply %v counter %v", rf.me, rf.state, rf.Curr_term, args, reply, rf.counter)
	// step 1:
	// args term must be bigger than curr_term
	if args.Term < rf.Curr_term {
		reply.Success = false
		reply.Term = rf.Curr_term
		// NOTE save
		rf.persist()
		return
	}
	if args.Term > rf.Curr_term {
		rf.Curr_term = args.Term
		rf.Vote_for = -1
	}
	rf.ChangeState(Follower)
	reply.Term = rf.Curr_term
	// log duplication
	DPrintf("{Node %v} term %v ,nowloglength  %v ,args.loglength %v,args.prevlogidx %v args.LeaderCommit %v rf.commit %v", rf.me, rf.Curr_term, len(rf.Log), len(args.Entries), args.PrevLogIdx, args.LeaderCommit, rf.commit_idx)

	// step 2: does not contain prevlog
	if rf.GetRealLogLen()-1 < args.PrevLogIdx {
		// when a follower not recv A log  in term n
		// A log commit
		// then recv hb in term n+1
		// this follower should duplicate A log by heartbeat
		reply.Success = false
	} else if rf.Log[rf.GetRealIdx(args.PrevLogIdx)].Term != args.PrevLogTerm {
		reply.Success = false
	} else {
		DPrintf("{Node %v} term %v state %v args.prevlogidx %v GetRealIdx(args.prevlogidx) %v log[rf.GetRealIdx(args.prevlogidx)].index %v", rf.me, rf.Curr_term, rf.state, args.PrevLogIdx, rf.GetRealIdx(args.PrevLogIdx), rf.Log[rf.GetRealIdx(args.PrevLogIdx)].Index)
		assert(args.PrevLogIdx == rf.Log[rf.GetRealIdx(args.PrevLogIdx)].Index)
		DPrintf("args.prevlogidx %v arg.prevlogterm %v prevlogidx %v prevlogterm %v", args.PrevLogIdx, args.PrevLogTerm, len(rf.Log)-1, rf.Log[rf.GetRealIdx(args.PrevLogIdx)].Term)
		DPrintf("now prevlogterm %v,args.prevlogterm %v", rf.Log[rf.GetRealIdx(args.PrevLogIdx)].Term, args.PrevLogTerm)
		assert(rf.Log[rf.GetRealIdx(args.PrevLogIdx)].Term == args.PrevLogTerm)
		// NOTE:
		//  1 2 3 peer ,1 leader
		//  abc log  commit on 1 2 peer ,not commit but copy to 3 peer
		//  1 ,2  commit_idx is 1 ,3 is 0
		//  1 crash ,then 3 could be leader
		//  in this case 3 LeaderCommit < 2 commit_idx

		// NOTE:
		// log duplication not just append
		rf.LogDuplicate(args.PrevLogIdx, args.Entries)
		DPrintf("{Node %v} term %v commit_idx %v last_applied %v args.LeaderCommit %v loglength %v", rf.me, rf.Curr_term, rf.commit_idx, rf.last_applied, args.LeaderCommit, len(rf.Log)-1)

		if args.LeaderCommit > rf.commit_idx {
			rf.commit_idx = Min(args.LeaderCommit, rf.GetRealLogLen()-1)
		}
		if rf.commit_idx > rf.last_applied {
			DPrintf("{Node %v} term %v commit_idx %v last_applied %v signal", rf.me, rf.Curr_term, rf.commit_idx, rf.last_applied)
			rf.cond_var.Signal()
		}
		reply.Success = true
	}
	// NOTE save
	rf.persist()
	rf.counter = rf.GetBigCounter()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	entry := Entry{
		Term:  rf.Curr_term,
		Cmd:   command,
		Index: rf.next_log_idx,
	}
	rf.next_log_idx += 1
	rf.Log = append(rf.Log, entry)
	DPrintf("{Node %v} term %v state %v loglength %v next_log_idx %v", rf.me, rf.Curr_term, rf.state, len(rf.Log), rf.next_log_idx)
	assert(rf.GetRealLogLen() == rf.next_log_idx)
	// NOTE:
	// appendentries background (by heartbeat)
	index = entry.Index
	term = entry.Term
	DPrintf("---------")
	DPrintf("msg : %v", entry)
	DPrintf("---------")
	// NOTE save
	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	timer := time.NewTimer(30 * time.Millisecond)
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		<-timer.C
		rf.mu.Lock()
		rf.counter -= 1
		switch rf.state {
		case Follower:
			fallthrough
		case Candidate:
			if rf.counter == 0 {
				rf.Curr_term += 1
				rf.ChangeState(Candidate)
				rf.persist()
				rf.counter = rf.GetBigCounter()
				rf.StartElect()
			}
		case Leader:
			if rf.counter == 0 {
				rf.BroadCastHB()
				rf.counter = rf.GetSmallCounter()
			}
		}
		// DPrintf("{Node %v} term %v state %v counter %v", rf.me, rf.Curr_term, rf.state, rf.counter)
		rf.mu.Unlock()

		timer.Reset(30 * time.Millisecond)
		// pause for a random amount of time between 50 and 350
		// milliseconds.
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:              peers,
		persister:          persister,
		state:              Follower,
		me:                 me,
		Curr_term:          0,
		Vote_for:           -1,
		Log:                []Entry{Entry{Term: 0, Cmd: "None", Index: 0}},
		commit_idx:         0,
		last_applied:       0,
		follower_next_idx:  make([]int, len(peers)),
		follower_match_idx: make([]int, len(peers)),
		vote_recv:          0,
		next_log_idx:       1,
		num_most:           len(peers)/2 + 1,
		appl_ch:            applyCh,
		snapshot_idx:       0,
		prev_snapshot_idx:  0,
		snapshot_term:      0,
		SnapShotData:       nil,
		fromTop:            false,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	for i := range rf.follower_next_idx {
		rf.follower_next_idx[i] = 1
		rf.follower_match_idx[i] = 0
	}
	rf.cond_var = *sync.NewCond(&rf.mu)

	// Your initialization code here (3A, 3B, 3C).
	rf.counter = rf.GetBigCounter()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyRoutine()

	// NOTE:
	// lab3a set timer
	return rf
}
