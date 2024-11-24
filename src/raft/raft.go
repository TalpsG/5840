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
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	curr_term int
	vote_for  int
	log       []Entry

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

	is_heartbeating []bool

	// half +1
	num_most int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.curr_term, rf.state == Leader
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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing RequestVote,  RequestVoteArgs %v and RequestVoteReply %v , vote_for %v", rf.me, rf.state, rf.curr_term, args, reply, rf.vote_for)
	if args.Term < rf.curr_term || (args.Term == rf.curr_term && rf.vote_for != -1 && rf.vote_for != args.CandidateId) {
		// 1. term 小
		// 2. 已经投过票了
		reply.Term = rf.curr_term
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.curr_term {
		rf.ChangeState(Follower)
		rf.curr_term = args.Term
	}
	reply.Term = args.Term
	rf.counter = rf.GetElectionCount()
	if rf.GetLastLog().Term > args.LastLogTerm {
		reply.VoteGranted = false
		rf.vote_for = -1
		DPrintf("{Node %v} term %v ,last term %v ,args.last_term %v", rf.me, rf.curr_term, rf.GetLastLog().Term, args.LastLogTerm)
		return
	}
	if rf.GetLastLog().Term == args.LastLogTerm && rf.GetLastLog().Index > args.LastLogIndex {
		reply.VoteGranted = false
		rf.vote_for = -1
		DPrintf("{Node %v} term %v ,last idx  %v ,args.last_idx  %v", rf.me, rf.curr_term, rf.GetLastLog().Index, args.LastLogIndex)
		return

	}
	rf.vote_for = args.CandidateId
	reply.VoteGranted = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing AppendEntries,  AppendEntriesArgs %v and AppendEntriesReply %v counter %v", rf.me, rf.state, rf.curr_term, args, reply, rf.counter)
	if args.Term < rf.curr_term {
		reply.Success = false
		reply.Term = rf.curr_term
		return
	}
	if args.Term > rf.curr_term {
		rf.curr_term = args.Term
		rf.vote_for = -1
	}
	rf.ChangeState(Follower)
	reply.Term = rf.curr_term
	if len(args.Entries) != 0 {
		// log duplication
		DPrintf("{Node %v} term %v ,nowloglength  %v ,args.loglength %v,args.prevlogidx %v", rf.me, rf.curr_term, len(rf.log), len(args.Entries), args.PrevLogIdx)
		// NOTE:
		// this assert cannot use at this situation
		// which peer may miss a log duplication
		assert(rf.log[args.PrevLogIdx].Term == args.PrevLogTerm)
		assert(rf.commit_idx <= args.LeaderCommit)
		rf.log = append(rf.log, args.Entries...)
		rf.commit_idx = Min(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Success = true
	rf.counter = rf.GetElectionCount()
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
		Term:  rf.curr_term,
		Cmd:   command,
		Index: rf.next_log_idx,
	}
	rf.next_log_idx += 1
	rf.log = append(rf.log, entry)
	assert(len(rf.log) == (rf.next_log_idx))
	// NOTE:
	// appendentries background (by heartbeat)

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
				rf.curr_term += 1
				rf.ChangeState(Candidate)
				rf.counter = rf.GetElectionCount()
				rf.StartElect()
			}
		case Leader:
			if rf.counter == 0 {
				rf.BroadCastHB()
				rf.counter = rf.GetHBCounter()
			}
		}
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
		curr_term:          0,
		vote_for:           -1,
		log:                []Entry{Entry{Term: -1, Cmd: "None"}},
		commit_idx:         0,
		last_applied:       0,
		follower_next_idx:  make([]int, len(peers)),
		follower_match_idx: make([]int, len(peers)),
		vote_recv:          0,
		next_log_idx:       1,
		is_heartbeating:    make([]bool, len(peers)),
		num_most:           len(peers)/2 + 1,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	for i := range rf.follower_next_idx {
		rf.follower_next_idx[i] = 1
		rf.follower_match_idx[i] = 0
	}

	// Your initialization code here (3A, 3B, 3C).
	rf.counter = rf.GetElectionCount()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// NOTE:
	// lab3a set timer
	return rf
}
