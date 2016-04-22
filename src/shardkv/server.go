package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

/*=======================Structs and Consts=====================*/

const Debug = 1

type Op struct {
	Key    string
	Value  string
	Op     string
	Shard  int
	Jid    int
	Who    int64
	UUID   int64
	Data   map[string]string
	Seen   map[int64]int
	Config shardmaster.Config
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	gid        int64 // my replica group ID
	applied    int   // applied paxos seq
	db         map[string]string
	seen       map[int]map[int64]int // processed request, grouped by shard, then by individual client
	replied    map[int64]string      // lastest reply value to the client Get request
	received   map[int]map[int]bool  // received shard data for each config change
	config     shardmaster.Config
}

/*========================Helper Functions======================*/

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) check_status(seq int) (paxos.Fate, Op) {
	to := 10 * time.Millisecond
	for {
		status, op := kv.px.Status(seq)
		if status == paxos.Decided {
			return status, op.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		} else {
			return status, Op{}
		}
	}
}

func (kv *ShardKV) apply(op Op) (Err, int64, string) {
	if op.Op == "SendShard" {
		kv.send_shard(op.Config)
	} else if op.Op == "Reconfig" {
		kv.load_shard(op)
	} else if op.Op == "ChangeConfig" {
		kv.change_config(op.Config)
	} else if op.Jid > kv.seen[op.Shard][op.Who] {
		if len(kv.seen[op.Shard]) == 0 {
			kv.seen[op.Shard] = make(map[int64]int)
		}
		kv.seen[op.Shard][op.Who] = op.Jid
		switch op.Op {
		case "Put":
			DPrintf("[%d --> %d] [Put] [Key: %s] [Value: %s]\n", kv.gid, kv.me, op.Key, op.Value)
			kv.db[op.Key] = op.Value
		case "Append":
			DPrintf("[%d --> %d] [Append] [Key: %s] [Value: %s]\n", kv.gid, kv.me, op.Key, op.Value)
			kv.db[op.Key] += op.Value
		case "Get":
			if _, exist := kv.db[op.Key]; !exist {
				DPrintf("[%d --> %d] [Err] [No Key: %s]\n", kv.gid, kv.me, op.Key)
				return ErrNoKey, op.UUID, ""
			} else {
				value := kv.db[op.Key]
				DPrintf("[%d --> %d] [Get] [Key: %s] [Value: %s]\n", kv.gid, kv.me, op.Key, value)
				return OK, op.UUID, value
			}
		}
	}
	kv.applied++
	kv.px.Done(kv.applied)
	return OK, op.UUID, ""
}

// Actual function to deal with client requests
func (kv *ShardKV) do(op Op) (Err, string) {
	if op.Op != "SendShard" && kv.config.Shards[op.Shard] != kv.gid && op.Op != "Reconfig" && op.Op != "ChangeConfig" {
		DPrintf("[%d --> %d] [Err] [%v] [K: %v] [Shard Gid %d :: KV Gid %d]\n", kv.gid, kv.me, op.Op, op.Key, kv.config.Shards[op.Shard], kv.gid)
		return ErrWrongGroup, ""
	}
	for true {
		seq := kv.applied + 1
		status, v := kv.px.Status(seq)
		var decision Op
		if status == paxos.Decided {
			decision = v.(Op)
		} else {
			kv.px.Start(seq, op)
			_, decision = kv.check_status(seq)
		}
		ok, UUID, value := kv.apply(decision)
		if UUID == op.UUID {
			return ok, value
		}
	}
	return OK, ""
}

/*==================Clinet Requests Handlers================*/

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var op Op
	op.Key = args.Key
	op.Op = "Get"
	op.Shard = args.Shard
	op.Jid = args.Jid
	op.Who = args.Who
	op.UUID = nrand()
	err, v := kv.do(op)
	switch err {
	case ErrNoKey:
		reply.Err = ErrNoKey
	case ErrWrongGroup:
		reply.Err = ErrWrongGroup
	case OK:
		if v == "" {
			v = kv.replied[op.Who]
		} else {
			kv.replied[op.Who] = v
		}
		reply.Err = OK
		reply.Value = v
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var op Op
	op.Key = args.Key
	op.Value = args.Value
	op.Op = args.Op
	op.Shard = args.Shard
	op.Jid = args.Jid
	op.Who = args.Who
	op.UUID = nrand()
	err, _ := kv.do(op)
	switch err {
	case ErrWrongGroup:
		reply.Err = ErrWrongGroup
	case OK:
		DPrintf("[%d --> %d] [Done] [%s] [Key: %s] [Value: %s]\n", kv.gid, kv.me, args.Op, args.Key, args.Value)
		reply.Err = OK
	}
	return nil
}

/*================Reconfiguration=================*/

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	new_config := kv.sm.Query(-1)
	DPrintf("[%d --> %d] [Tick] [New Config Num: %d :: Old Config Num: %d]\n", kv.gid, kv.me, new_config.Num, kv.config.Num)
	var op Op
	op.Op = "SendShard"
	op.Config = new_config
	op.UUID = nrand()
	kv.do(op)
	op.UUID = nrand()
	op.Op = "ChangeConfig"
	kv.do(op)
}

func (kv *ShardKV) change_config(new_config shardmaster.Config) {
	if new_config.Num > kv.config.Num {
		DPrintf("[%d --> %d] [Change Config] [%d --> %d]\n", kv.gid, kv.me, kv.config.Num, new_config.Num)
		kv.config = new_config
	}
}

func (kv *ShardKV) send_shard(new_config shardmaster.Config) {
	old_config := kv.config
	for shard, gid := range new_config.Shards {
		// the shard originally assign to me is assigned to others
		// send the data to the new owner of the shard
		if old_config.Shards[shard] == kv.gid && gid != kv.gid {
			data := kv.pack(shard)
			args := &SendShardArgs{}
			args.ConfigNum = new_config.Num
			args.Shard = shard
			args.Data = data
			args.Seen = kv.seen[shard]
			for _, srv := range new_config.Groups[gid] {
				DPrintf("[%d --> %d] [Config %d -> Config %d] [Send Shard %d -> Group %d]\n", kv.gid, kv.me, old_config.Num, new_config.Num, shard, gid)
				var reply SendShardReply
				ok := call(srv, "ShardKV.Reconfig", args, &reply)
				DPrintf("Here\n")
				if ok && reply.Err == ErrOldConfig {
					DPrintf("[%d --> %d] [SendShard] [Err] [Receiver Has More Recent Config]\n", kv.gid, kv.me)
				}
				if ok {
					break
				}
			}
			//if ok && reply.Err == ErrWrongGroup {
			//DPrintf("[ShardKV %d] [SendShard] [Err] [Receiver Not Ready]\n", kv.me)
			//}
		}
	}
}

// pack db data of a shard for transferring to another group
func (kv *ShardKV) pack(shard int) map[string]string {
	data := make(map[string]string)
	for key, value := range kv.db {
		if key2shard(key) == shard {
			data[key] = value
		}
	}
	return data
}

// handler for receiving newly assigned shard data
func (kv *ShardKV) Reconfig(args *SendShardArgs, reply *SendShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	configNum, shard, data, seen := args.ConfigNum, args.Shard, args.Data, args.Seen
	if configNum < kv.config.Num {
		reply.Err = ErrOldConfig
		DPrintf("[%d --> %d] [SendShard] [Err] [Has More Recent Config]\n", kv.gid, kv.me)
		return nil
	}
	if len(kv.received[configNum]) == 0 {
		kv.received[configNum] = make(map[int]bool)
	}
	if kv.received[configNum][shard] == false {
		DPrintf("[%d --> %d] [Got Shard %d Data]\n", kv.gid, kv.me, shard)
		kv.received[configNum][shard] = true
		var op Op
		op.Op = "Reconfig"
		op.Shard = shard
		op.Data = data
		op.Seen = seen
		op.UUID = nrand()
		kv.do(op)
		DPrintf("[%d --> %d] [Finished Loading Shard %d]\n", kv.gid, kv.me, shard)
	}
	return nil
}

// actual function for receiver end to process shard data
func (kv *ShardKV) load_shard(op Op) {
	shard, data, seen := op.Shard, op.Data, op.Seen
	DPrintf("[%d --> %d] [Load Shard %d]\n", kv.gid, kv.me, shard)
	kv.seen[shard] = seen
	for key := range data {
		value := data[key]
		DPrintf("[%d --> %d] [Loading Shard %d] [K: %v ---> V: %v]\n", kv.gid, kv.me, shard, key, value)
		kv.db[key] = value
	}
}

/*=================Do Not Modify==================*/

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.db = make(map[string]string)
	kv.seen = make(map[int]map[int64]int)
	kv.replied = make(map[int64]string)
	kv.received = make(map[int]map[int]bool)
	kv.applied = 0
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.config = shardmaster.Config{Num: 0}

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
