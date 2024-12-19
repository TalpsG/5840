package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Operation int

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type Record struct {
	CmdId     int64
	Operation string
	LastReply ExecuteCmdReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	records    map[int64]*Record
	db         map[string]string
	result_ch  map[int]chan *ExecuteCmdReply
	last_apply int
}

func (kv *KVServer) Lock() {
	DPrintf("{Server %v} Locking", kv.me)
	kv.mu.Lock()
}

func (kv *KVServer) Unlock() {
	DPrintf("{Server %v} Unlock", kv.me)
	kv.mu.Unlock()
}
func (kv *KVServer) Get(args *ExecuteCmdArgs, reply *ExecuteCmdReply) {
	// Your code here.
	DPrintf("{Server %v} rpc %v from client_id %v", kv.me, args.Operation, args.ClientId)
	kv.Lock()
	if r, ok := kv.records[args.ClientId]; ok && r.CmdId >= args.CmdId {
		// If this cmd is redundant(cmdId < record.cmdid)
		DPrintf("{Server %v} old %v client_id %v cmd_id %v key %v value %v", kv.me, args.Operation, args.ClientId, args.CmdId, args.Key, r.LastReply.Value)
		assert(r.LastReply.Erro == OK)
		*reply = r.LastReply
		kv.Unlock()
		return
	}
	kv.Unlock()
	index, _, is_leader := kv.rf.Start(*args)
	if !is_leader {
		reply.Erro = ErrWrongLeader
		reply.Value = ""
		return
	}
	DPrintf("{Server %v} submit log index %v", kv.me, index)
	// wait for log apply
	// applier will send applied log by result_ch
	kv.Lock()
	ch := kv.GetResultChannel(index)
	kv.Unlock()
	applied := <-ch

	assert(applied.Erro == OK)

	reply.Erro = applied.Erro
	reply.Value = applied.Value
	go func() {
		kv.Lock()
		kv.DeleteResultChannel(index)
		defer kv.Unlock()
	}()
	DPrintf("{Server %v} reply index %v reply %v", kv.me, index, *reply)
}

func (kv *KVServer) ApplyLog(args *ExecuteCmdArgs) string {
	switch args.Operation {
	case "Get":
		return kv.db[args.Key]
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
		kv.db[args.Key] += args.Value
	default:
		panic(fmt.Sprintf("unknown Operation %v", args.Operation))
	}
	return ""
}

func (kv *KVServer) Put(args *ExecuteCmdArgs, reply *ExecuteCmdReply) {
	// Your code here.
	DPrintf("{Server %v} rpc %v from client_id %v", kv.me, args.Operation, args.ClientId)
	kv.Lock()
	if r, ok := kv.records[args.ClientId]; ok && r.CmdId >= args.CmdId {
		// If this cmd is redundant(cmdId < record.cmdid)
		assert(r.LastReply.Erro == OK)
		*reply = r.LastReply
		DPrintf("{Server %v} old %v client_id %v cmd_id %v key %v value %v", kv.me, args.Operation, args.ClientId, args.CmdId, args.Key, reply.Value)
		kv.Unlock()
		return
	}
	kv.Unlock()
	index, _, is_leader := kv.rf.Start(*args)
	if !is_leader {
		reply.Erro = ErrWrongLeader
		reply.Value = ""
		return
	}
	DPrintf("{Server %v} submit log index %v", kv.me, index)
	// wait for log apply
	// no need to consider about snapshot
	kv.Lock()
	ch := kv.GetResultChannel(index)
	kv.Unlock()
	applied := <-ch
	assert(applied.Erro == OK)
	reply.Erro = applied.Erro
	go func() {
		kv.Lock()
		kv.DeleteResultChannel(index)
		defer kv.Unlock()
	}()
	DPrintf("{Server %v} reply index %v reply %v", kv.me, index, *reply)
}

func (kv *KVServer) Append(args *ExecuteCmdArgs, reply *ExecuteCmdReply) {
	/// Your code here.
	DPrintf("{Server %v} rpc %v from client_id %v", kv.me, args.Operation, args.ClientId)
	kv.Lock()
	if r, ok := kv.records[args.ClientId]; ok && r.CmdId >= args.CmdId {
		// If this cmd is redundant(cmdId < record.cmdid)
		assert(r.LastReply.Erro == OK)
		*reply = r.LastReply
		DPrintf("{Server %v} old %v client_id %v cmd_id %v key %v value %v", kv.me, args.Operation, args.ClientId, args.CmdId, args.Key, reply.Value)
		kv.Unlock()
		return
	}
	kv.Unlock()
	index, _, is_leader := kv.rf.Start(*args)
	if !is_leader {
		reply.Erro = ErrWrongLeader
		return
	}
	DPrintf("{Server %v} submit log index %v", kv.me, index)
	// wait for log apply
	// no need to consider about snapshot
	kv.Lock()
	ch := kv.GetResultChannel(index)
	kv.Unlock()
	applied := <-ch
	assert(applied.Erro == OK)
	reply.Erro = applied.Erro
	go func() {
		kv.Lock()
		kv.DeleteResultChannel(index)
		defer kv.Unlock()
	}()
	DPrintf("{Server %v} reply index %v reply %v", kv.me, index, *reply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ExecuteCmdArgs{})
	labgob.Register(ExecuteCmdReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.result_ch = make(map[int]chan *ExecuteCmdReply)
	kv.db = make(map[string]string)
	kv.last_apply = 0
	kv.records = make(map[int64]*Record)

	// You may need initialization code here.
	go func() {
		for {
			msg := <-kv.applyCh
			DPrintf("{Server %v} get a msg %v", kv.me, msg.Command)
			assert(msg.CommandValid)
			DPrintf("{Server %v} applier recv index %v cmd %v", kv.me, msg.CommandIndex, msg.Command)
			kv.Lock()
			assert(kv.last_apply < msg.CommandIndex)
			args := msg.Command.(ExecuteCmdArgs)
			DPrintf("{Server %v} apply log %v", kv.me, args)
			ret := kv.ApplyLog(&args)
			if r, ok := kv.records[args.ClientId]; !ok || r == nil {
				kv.records[args.ClientId] = &Record{
					LastReply: ExecuteCmdReply{Erro: OK},
				}
			}
			kv.records[args.ClientId].CmdId = args.CmdId
			kv.records[args.ClientId].Operation = args.Operation
			if args.Operation == "Get" {
				kv.records[args.ClientId].LastReply.Value = ret
			}
			DPrintf("{Server %v} store last reply %v", kv.me, *kv.records[args.ClientId])
			kv.last_apply = msg.CommandIndex
			if _, is_leader := kv.rf.GetState(); is_leader {
				DPrintf("{Server %v} client_id %v cmd_id %v reply %v sending", kv.me, args.ClientId, args.CmdId, kv.records[args.ClientId].LastReply)
				result_ch := kv.GetResultChannel(msg.CommandIndex)
				result_ch <- &kv.records[args.ClientId].LastReply
			}
			DPrintf("{Server %v} client_id %v cmd_id %v reply %v", kv.me, args.ClientId, args.CmdId, kv.records[args.ClientId].LastReply)
			kv.Unlock()
		}
	}()

	return kv
}
func (kv *KVServer) DeleteResultChannel(index int) {
	DPrintf("{Server %v} delete result_ch %v ", kv.me, index)
	delete(kv.result_ch, index)
}
func (kv *KVServer) GetResultChannel(index int) chan *ExecuteCmdReply {
	if _, ok := kv.result_ch[index]; !ok {
		DPrintf("{Server %v} create result_ch %v ", kv.me, index)
		kv.result_ch[index] = make(chan *ExecuteCmdReply)
	}
	DPrintf("{Server %v} get result_ch %v ", kv.me, index)
	return kv.result_ch[index]
}
