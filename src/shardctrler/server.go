package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Reply struct {
	Err    Err
	Config Config
}

type Session struct {
	CmdID    int64
	OpType   string
	Respones Reply
}
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	sessions    map[int64]Session
	configs     []Config // indexed by config num
	notifyMapCh map[int]chan Reply
}

type Op struct {
	// Your data here.
	ClientID  int64
	CmdId     int64
	OpType    string
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	ConfigNum int
}

func (sc *ShardCtrler) CreateCh(index int) chan Reply {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch := make(chan Reply, 1)
	sc.notifyMapCh[index] = ch
	return ch
}
func (sc *ShardCtrler) DeleteCh(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, ok := sc.notifyMapCh[index]; ok {
		close(sc.notifyMapCh[index])
		delete(sc.notifyMapCh, index)
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if args.CmdId < sc.sessions[args.ClientId].CmdID {
		// 很老的请求，client已经得到过结果了，只是重发的请求太慢了现在才处理
		sc.mu.Unlock()
		return
	} else if args.CmdId == sc.sessions[args.ClientId].CmdID {
		reply.Err = sc.sessions[args.ClientId].Respones.Err
		sc.mu.Unlock()
		return
	} else {
		sc.mu.Unlock()
		op := Op{
			ClientID: args.ClientId,
			CmdId:    args.CmdId,
			OpType:   Join,
			Servers:  args.Servers,
		}
		index, _, isLeader := sc.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			return
		}
		ch := sc.CreateCh(index)
		select {
		case res := <-ch:
			reply.Err = res.Err
		case <-time.After(Timeout * time.Millisecond):
			reply.Err = ErrTimeout
		}
		go sc.DeleteCh(index)
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.CmdId < sc.sessions[args.ClientId].CmdID {
		// 很老的请求，client已经得到过结果了，只是重发的请求太慢了现在才处理
		sc.mu.Unlock()
		return
	} else if args.CmdId == sc.sessions[args.ClientId].CmdID {
		reply.Err = sc.sessions[args.ClientId].Respones.Err
		sc.mu.Unlock()
		return
	} else {
		sc.mu.Unlock()
		op := Op{
			ClientID: args.ClientId,
			CmdId:    args.CmdId,
			OpType:   Leave,
			GIDs:     args.GIDs,
		}
		index, _, isLeader := sc.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			return
		}
		ch := sc.CreateCh(index)
		select {
		case res := <-ch:
			reply.Err = res.Err
		case <-time.After(Timeout * time.Millisecond):
			reply.Err = ErrTimeout
		}
		go sc.DeleteCh(index)
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.CmdId < sc.sessions[args.ClientId].CmdID {
		// 很老的请求，client已经得到过结果了，只是重发的请求太慢了现在才处理
		sc.mu.Unlock()
		return
	} else if args.CmdId == sc.sessions[args.ClientId].CmdID {
		reply.Err = sc.sessions[args.ClientId].Respones.Err
		sc.mu.Unlock()
		return
	} else {
		sc.mu.Unlock()
		op := Op{
			ClientID: args.ClientId,
			CmdId:    args.CmdId,
			OpType:   Move,
			Shard:    args.Shard,
			GID:      args.GID,
		}
		index, _, isLeader := sc.rf.Start(op)
		if !isLeader {
			reply.WrongLeader = true
			return
		}
		ch := sc.CreateCh(index)
		select {
		case res := <-ch:
			reply.Err = res.Err
		case <-time.After(Timeout * time.Millisecond):
			reply.Err = ErrTimeout
		}
		go sc.DeleteCh(index)
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		ClientID:  args.ClientId,
		CmdId:     args.CmdId,
		OpType:    Query,
		ConfigNum: args.Num,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := sc.CreateCh(index)
	select {
	case res := <-ch:
		reply.Err = res.Err
		reply.Config = res.Config
	case <-time.After(Timeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go sc.DeleteCh(index)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Killed() bool {
	// Your code here, if desired.
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.sessions = make(map[int64]Session)
	sc.notifyMapCh = make(map[int]chan Reply)

	go sc.ApplyRoutine()
	return sc
}
func (sc *ShardCtrler) ApplyRoutine() {
	for !sc.Killed() {
		msg := <-sc.applyCh
		DPrintf("msg %v", msg)
		if msg.CommandValid {
			sc.mu.Lock()
			op, ok := msg.Command.(Op)
			if !ok {
				DPrintf("cmd %v", msg.Command)
				panic("unknown cmd")
			}
			reply := Reply{}
			sessionRec, exist := sc.sessions[op.ClientID]
			if exist && op.OpType != Query && op.CmdId <= sessionRec.CmdID {
				reply = sc.sessions[op.ClientID].Respones
			} else {
				switch op.OpType {
				case Join:
					reply.Err = sc.executeJoin(op)
					DPrintf("{shardServer %v} %v cmdid %v op %v success", sc.me, op.OpType, op.CmdId, op)
				case Move:
					reply.Err = sc.executeMove(op)
					DPrintf("{shardServer %v} %v cmdid %v op %v success", sc.me, op.OpType, op.CmdId, op)
				case Leave:
					reply.Err = sc.executeLeave(op)
					DPrintf("{shardServer %v} %v cmdid %v op %v success", sc.me, op.OpType, op.CmdId, op)
				case Query:
					reply.Err, reply.Config = sc.executeQuery(op)
					DPrintf("{shardServer %v} %v cmdid %v op %v success", sc.me, op.OpType, op.CmdId, op)
				default:
					DPrintf("unknown optype %v", op.OpType)
				}
				if op.OpType != Query {
					session := Session{
						CmdID:    op.CmdId,
						OpType:   op.OpType,
						Respones: reply,
					}
					sc.sessions[op.ClientID] = session
					DPrintf("{shardServer %v} store session clientid %v cmdid %v reply %v", sc.me, op.ClientID, op.CmdId, reply)
				}
			}
			if _, exist := sc.notifyMapCh[msg.CommandIndex]; exist {
				if term, isLeader := sc.rf.GetState(); isLeader && msg.CommandTerm == term {
					sc.notifyMapCh[msg.CommandIndex] <- reply
				}
			}
			sc.mu.Unlock()

		} else {
			panic("shardctrler no snapshot")
		}
	}
}
func (sc *ShardCtrler) executeJoin(op Op) Err {
	last := sc.getLastConfig()
	newconfig := Config{
		Num: last.Num + 1,
	}
	newgroups := deepCopyGroups(last.Groups)
	for gid, servers := range op.Servers {
		newgroups[gid] = servers
	}
	newconfig.Groups = newgroups

	newconfig.Shards = shardBalance(newgroups, last.Shards)
	sc.configs = append(sc.configs, newconfig)
	fmt.Println(newconfig.Shards)
	return OK
}
func (sc *ShardCtrler) executeQuery(op Op) (Err, Config) {
	lastconfig := sc.getLastConfig()
	if op.ConfigNum == -1 || op.ConfigNum > lastconfig.Num {
		return OK, lastconfig
	}
	return OK, sc.configs[op.ConfigNum]
}
func (sc *ShardCtrler) executeLeave(op Op) Err {
	lastconfig := sc.getLastConfig()
	newconfig := Config{}
	newconfig.Num = lastconfig.Num + 1

	newgroups := deepCopyGroups(lastconfig.Groups)
	for _, gid := range op.GIDs {
		delete(newgroups, gid)
	}

	newconfig.Groups = newgroups
	var newShards [NShards]int
	if len(newgroups) != 0 {
		newShards = shardBalance(newconfig.Groups, lastconfig.Shards)
	}
	newconfig.Shards = newShards
	sc.configs = append(sc.configs, newconfig)
	return OK
}
func (sc *ShardCtrler) executeMove(op Op) Err {
	lastconfig := sc.getLastConfig()
	newconfig := Config{}
	newconfig.Num = lastconfig.Num + 1
	newconfig.Groups = deepCopyGroups(lastconfig.Groups)

	newshard := lastconfig.Shards
	newshard[op.Shard] = op.GID
	newconfig.Shards = newshard

	sc.configs = append(sc.configs, newconfig)
	return OK
}

func deepCopyGroups(src map[int][]string) map[int][]string {
	ret := make(map[int][]string)
	for gid, servers := range src {
		replica := make([]string, len(servers))
		copy(replica, servers)
		ret[gid] = replica
	}
	return ret
}
func (sc *ShardCtrler) getLastConfig() Config {
	return sc.configs[len(sc.configs)-1]
}
func shardBalance(groups map[int][]string, lastshards [NShards]int) [NShards]int {
	resShards := lastshards
	groupNum := len(groups)
	shardCnt := make(map[int]int, groupNum)

	for i, gid := range lastshards {
		if _, exist := groups[gid]; exist {
			shardCnt[gid]++
		} else {
			resShards[i] = 0
		}
	}

	// 此时shardCnt[gid] 中记录着 gid 负责了多少个分片
	// resShards[i] = 0 则代表着 该分片没有对应的group

	// gidslice记录所有gid
	// shardcnt[gid] 为0 则表示对应的group没有负责的shard
	gidSlice := make([]int, 0, groupNum)
	for gid, _ := range groups {
		gidSlice = append(gidSlice, gid)
		if _, exist := shardCnt[gid]; !exist {
			shardCnt[gid] = 0
		}
	}

	avg := NShards / groupNum
	remain := NShards % groupNum
	sort.Slice(gidSlice, func(i, j int) bool {
		if shardCnt[gidSlice[i]] > shardCnt[gidSlice[j]] {
			return true
		}
		if shardCnt[gidSlice[i]] == shardCnt[gidSlice[j]] {
			return gidSlice[i] < gidSlice[j]
		}
		return false
	})

	for i := 0; i < groupNum; i++ {
		var cur_num int
		if i < remain {
			cur_num = avg + 1
		} else {
			cur_num = avg
		}
		cur_gid := gidSlice[i]
		delta := shardCnt[cur_gid] - cur_num
		if delta == 0 {
			continue
		}
		if delta > 0 {
			for j := 0; j < NShards; j++ {
				if delta == 0 {
					break
				}
				if resShards[j] == cur_gid {
					resShards[j] = 0
					delta--
				}
			}

		}
	}
	for i := 0; i < groupNum; i++ {
		var cur_num int
		if i < remain {
			cur_num = avg + 1
		} else {
			cur_num = avg
		}
		cur_gid := gidSlice[i]
		delta := shardCnt[cur_gid] - cur_num
		if delta == 0 {
			continue
		}
		if delta < 0 {
			for j := 0; j < NShards; j++ {
				if delta == 0 {
					break
				}
				if resShards[j] == 0 {
					resShards[j] = cur_gid
					delta++
				}
			}
		}

	}
	return resShards
}
