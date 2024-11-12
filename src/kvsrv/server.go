package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]string

	record sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Msg == Report {
		// report rpc
		// delete store result
		kv.record.Delete(args.OpId)
		return
	}

	res, ok := kv.record.Load(args.OpId)
	if ok {
		// repeated put rpc
		reply.Value = res.(string)
		return
	}
	// put rpc first execute
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[args.Key] = args.Value
	kv.record.Store(args.OpId, "")
	reply.Value = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Msg == Report {
		// report rpc
		// delete store result
		kv.record.Delete(args.OpId)
		return
	}

	res, ok := kv.record.Load(args.OpId)
	if ok {
		// repeated put rpc
		reply.Value = res.(string)
		return
	}
	// put rpc first execute
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
	kv.data[args.Key] += args.Value
	kv.record.Store(args.OpId, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)

	return kv
}
