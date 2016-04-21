package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrOldConfig  = "ErrOldConfig"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Shard int
	Jid   int
	Who   int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key   string
	Shard int
	Jid   int
	Who   int64
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArgs struct {
	ConfigNum int
	Shard     int
	Data      map[string]string
	Seen      map[int64]int
}

type SendShardReply struct {
	Err Err
}
