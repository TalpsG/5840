package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	client_id int64
	cmd_id    int64
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
	// Your code here.
	ck.client_id = nrand()
	ck.cmd_id = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	DPrintf("{ShardClient %v} query %v cmd_id %v", ck.client_id, num, ck.cmd_id)
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.CmdId = ck.cmd_id
	args.ClientId = ck.client_id
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			DPrintf("{ShardClient %v} query response %v %v %v", ck.client_id, ok, reply.WrongLeader, reply.Err)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.cmd_id += 1
				DPrintf("{ShardClient %v} query %v response %v", ck.client_id, num, reply.Config)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	DPrintf("{ShardClient %v} join %v cmd_id %v", ck.client_id, servers, ck.cmd_id)
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.client_id
	args.CmdId = ck.cmd_id
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("{ShardClient %v} join response", ck.client_id)
				ck.cmd_id += 1
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	DPrintf("{ShardClient %v} leave %v cmd_id %v", ck.client_id, gids, ck.cmd_id)
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.client_id
	args.CmdId = ck.cmd_id

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("{ShardClient %v} leave response", ck.client_id)
				ck.cmd_id += 1
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	DPrintf("{ShardClient %v} move  shard %v gid %v cmd_id %v", ck.client_id, shard, gid, ck.cmd_id)
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.client_id
	args.CmdId = ck.cmd_id

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.cmd_id += 1
				DPrintf("{ShardClient %v} move response", ck.client_id)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
