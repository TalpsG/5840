package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader_id  int
	client_id  int64
	command_id int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.client_id = nrand()
	ck.command_id = 1
	return ck
}

func (ck *Clerk) ExecuteCmd(args *ExecuteCmdArgs) string {
	reply := ExecuteCmdReply{}
	for {
		DPrintf("{client %v} %v key %v cmd_id %v leader_id %v", ck.client_id, args.Operation, args.Key, args.CmdId, ck.leader_id)
		if !ck.servers[ck.leader_id].Call("KVServer."+args.Operation, args, &reply) || reply.Erro == ErrWrongLeader || reply.Erro == ErrTimeout {
			// if rpc fail or this node is not leader
			ck.leader_id = (ck.leader_id + 1) % len(ck.servers)
			continue
		}
		DPrintf("{client %v} %v success key %v cmd_id %v leader_id %v value %v", ck.client_id, args.Operation, args.Key, args.CmdId, ck.leader_id, reply.Value)
		ck.command_id += 1
		return reply.Value
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ck.ExecuteCmd(&ExecuteCmdArgs{
		Key:       key,
		Value:     "",
		Operation: "Get",
		ClientId:  ck.client_id,
		CmdId:     ck.command_id,
	})
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.ExecuteCmd(&ExecuteCmdArgs{
		Key:       key,
		Value:     value,
		Operation: op,
		ClientId:  ck.client_id,
		CmdId:     ck.command_id,
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
