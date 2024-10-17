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
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state     State
	curr_term int
	vote_for  int

	// when me is candidate
	// received vote_num
	vote_num int

	// timestamp of heartbeat or
	heartbeat bool
	// timestamp of
	has_vote bool
	// timeout for election or hb
	timer int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.curr_term
	isleader = rf.state == Leader
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
	// Your code here (2C).
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
	// Your code here (2C).
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
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Candidate_id int
	// check log
	Last_log_idx  int
	Last_log_term int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Vote_granted bool
	Term         int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. term
	if rf.curr_term > args.Term {
		reply.Term = rf.curr_term
		reply.Success = false
		rf.Message("curr_term > args.term , Leader_id : " + strconv.Itoa(args.Leader_id) + " leader_term : " + strconv.Itoa(args.Term))
		return
	}

	if args.Term == rf.curr_term && rf.state == Leader {
		rf.Message("2 leader , " + fmt.Sprint("leaderid", args.Leader_id, "args.term", args.Term))
		panic("2 leader")
	}
	rf.Message("curr_term < args.term , become follower , " + fmt.Sprint(args.Term, args.Leader_id))

	// 2. rf.log[Prev_log_idx] must have A LOG
	// 3. 从匹配的位置开始复制日志
	// 4. 添加新日志
	// 5. rf.commit_idx = min ( Leader_commit , newEntry_idx)

	if rf.curr_term < args.Term {
		rf.BecomeFollower(-1)
	} else {
		// term ==  curr_term
		if rf.state == Candidate {
			rf.BecomeFollower(rf.me)
		} else {
			// 保留此前投出去的票
			// 可以保证一个term只投一张票
			rf.BecomeFollower(rf.vote_for)
		}
	}
	rf.curr_term = args.Term
	rf.heartbeat = true
	reply.Success = true
	rf.Message("hb reply")
}

// example RequestVote RPC handler.
// NOTE:
// reply.term = max(rf.curr_term , args.term)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. args.term >= curr_term
	// 2. not vote or vote for src of request
	// 3. log newer
	term_bigger := args.Term > rf.curr_term
	term_equal := args.Term == rf.curr_term
	log_newer := rf.IsNewerLog(args)
	can_vote := rf.vote_for == -1 || rf.vote_for == args.Candidate_id

	if (rf.state == Candidate || rf.state == Leader) && rf.curr_term == args.Term && rf.vote_for != rf.me {
		rf.Message("only 1 vote in 1 term " + fmt.Sprint("args.term", args.Term, "id", args.Candidate_id))
		panic("only 1 vote in 1 term")
	}
	// 投票流程
	// vote for this candidate
	// 1. curr_term = args.term
	// 2. vote_for
	// 3. reply.vote_granted
	if term_bigger && log_newer {
		// NOTE:
		// term 大于且log_newer则直接投票
		rf.curr_term = args.Term
		rf.BecomeFollower(args.Candidate_id)
		reply.Vote_granted = true
		reply.Term = args.Term
		// record vote timestamp
		rf.has_vote = true
		rf.Message("term_bigger,log_newer : " + strconv.Itoa(rf.vote_for) + ":" + strconv.Itoa(args.Term))

	} else if term_equal && log_newer && can_vote {
		// NOTE:
		// 如果term 相同且log_newer则需要节点有票才可以投
		if can_vote && (rf.state == Candidate || rf.state == Leader) {
			rf.Message("candidate cannot vote")
			panic("candidate cannot vote")
		}
		rf.BecomeFollower(args.Candidate_id)
		rf.curr_term = args.Term
		reply.Vote_granted = true
		reply.Term = args.Term
		// record vote timestamp
		rf.has_vote = true
		rf.Message("term_equal,log_newer,can_vote : " + strconv.Itoa(rf.vote_for))
	} else {
		// 1. vote_granted = false
		reply.Vote_granted = false
		// 2. reply.term = rf.curr_term
		term_equal := args.Term == rf.curr_term
		vote_other := rf.vote_for != -1 && rf.vote_for != args.Candidate_id
		if term_equal && vote_other {
			// 2.1 this peer term == args.term
			// and has vote for other
			reply.Term = args.Term
			rf.Message("has vote other" + strconv.Itoa(rf.vote_for))
		} else {
			// 2.2 this peer term > args.term
			reply.Term = rf.curr_term
			rf.Message("curr_term > args.term" + fmt.Sprint("Candidate_id ", args.Candidate_id, " term ", args.Term))
		}
	}
	// NOTE: return val
	// reply.term = args.term
	// reply.vote_granted = true
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

type AppendEntriesArgs struct {
	Term          int
	Leader_id     int
	Prev_log_idx  int
	Prev_log_term int

	Leader_commit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
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

	// Your code here (2B).

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
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.timer = 200 + rand.Intn(300)
		rf.mu.Lock()
		switch rf.state {
		case Follower:
			// follower 需要关注自己是否收到了heartbeat
			// 如果收到了则继续当Follower，否则要变成candidate，进行选举
			if rf.heartbeat {
				rf.heartbeat = false
				rf.Message("follower recv hb")
			} else if rf.has_vote {
				// NOTE:
				// 模拟刷新定时器的操作
				// 如果投过票，则再等一个timeout再选举
				rf.has_vote = false
			} else {
				// follower -> candidate
				rf.Message("follower dont recv hb , start election")
				rf.Election()
			}
		case Candidate:
			// TODO:
			// 此处不检查自己的票数，票数放在RequestVote的goroutine当中判断
			// 因此此处必然是election timeout，需要进入下一轮选举
			rf.Message("candidate start next election")
			rf.Election()
		case Leader:
			// leader定时器应该是heartbeat_timer,到期
			rf.Message("leader BroadcastHeartBeat")
			rf.BroadcastHeartBeat()
			rf.timer = 100 + rand.Intn(150)
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(time.Duration(rf.timer) * time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.curr_term = 0
	rf.vote_for = -1
	rf.vote_num = -1
	rf.heartbeat = false
	rf.has_vote = false
	rf.state = Follower

	file, err := os.Open("log")
	if err != nil {
		fmt.Println("log open fail")
		os.Exit(1)
	}
	os.Stdout = file

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
func (rf *Raft) IsNewerLog(args *RequestVoteArgs) bool {
	return true
}

// NOTE:
// use within lock
func (rf *Raft) Election() {
	rf.curr_term += 1
	rf.vote_for = rf.me
	rf.heartbeat = false
	rf.state = Candidate
	rf.vote_num = 1
	rf.BroadcastVoteRequest()
}

// NOTE:
// use within lock
func (rf *Raft) BroadcastVoteRequest() {
	args := new(RequestVoteArgs)
	args.Term = rf.curr_term
	args.Candidate_id = rf.me
	// TODO:
	// need modify on lab2bcde
	args.Last_log_idx = -1
	args.Last_log_term = -1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, now int, args *RequestVoteArgs) {
			reply := RequestVoteReply{Vote_granted: false}
			ok := rf.sendRequestVote(i, args, &reply)
			// 收到rpc回复后要确定几件事
			// 1. rpc成功
			// 2. 发送和接收是同一个term，否则不会做任何修改
			// 3. reply.vote_granted
			// 4. state == candidate

			// NOTE:
			// this lock can make vote goroutine send rpc in timesleep
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok && reply.Vote_granted && now == rf.curr_term && rf.state == Candidate {
				// TODO:
				// RPC成功且获得选票
				// 检查票数，如果有足够的票了就进行状态转换
				// candidate -> leader
				rf.vote_num += 1
				if rf.vote_num >= (len(rf.peers)+1)/2 {
					rf.BecomeLeader()
					rf.BroadcastHeartBeat()
				}
			} else if ok && reply.Term > rf.curr_term {
				// 如果收到了reply.term > curr_term
				rf.curr_term = reply.Term
				rf.BecomeFollower(-1)
			}
		}(i, rf.curr_term, args)
	}
}

// NOTE:
// use within lock
func (rf *Raft) BecomeFollower(vote_for int) {
	rf.has_vote = false
	rf.heartbeat = false
	rf.state = Follower
	rf.vote_num = -1
	rf.vote_for = vote_for
}

// NOTE:
// use within lock
func (rf *Raft) BecomeLeader() {
	rf.vote_for = rf.me
	rf.has_vote = false
	rf.heartbeat = false
	rf.state = Leader
	rf.vote_num = -1
}
func (rf *Raft) BroadcastHeartBeat() {
	args := new(AppendEntriesArgs)
	args.Term = rf.curr_term
	args.Leader_id = rf.me
	// TODO:
	// need modify on lab2bcde
	args.Prev_log_idx = -1
	args.Prev_log_term = -1
	args.Leader_commit = -1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := AppendEntriesReply{}
			reply.Success = false
			rf.sendAppendEntries(i, args, &reply)
			// NOTE:
			// heartbeat 设计成了不关心是否到达
			// 因为心跳的目的是保持leader在线，log同步也可以做但没必要
		}(i)
	}
}

// NOTE:
// use within lock
func (rf *Raft) Message(msg string) {
	t := fmt.Sprintf("term : %3d , me : %d , state : %3d , vote_for : %3d , vote_num : %3d , hb : %v , ", rf.curr_term, rf.me, rf.state, rf.vote_for, rf.vote_num, rf.heartbeat)
	DPrintf(t + msg + "\n")
}
