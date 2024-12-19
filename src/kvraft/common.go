package kvraft

func assert(t bool) {
	if !t {
		panic("bool ")
	}
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type MessageType int

const (
	Modify = iota
	Report
)

type Err string

type ExecuteCmdArgs struct {
	Operation string
	Key       string
	Value     string
	ClientId  int64
	CmdId     int64
}
type ExecuteCmdReply struct {
	Erro  Err
	Value string
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId int64
	Msg  MessageType
}

type PutAppendReply struct {
	Err   Err
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
