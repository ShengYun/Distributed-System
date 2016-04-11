package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

/*===============Helper Functions====================*/
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

func (sm *ShardMaster) apply_op(op Op) {
	switch op.Action {
	case JOIN:
	case LEAVE:
	case MOVE:
	case QUERY:
	}
}

func (sm *ShardMaster) check_status(seq int) (paxos.Fate, Op) {
	to := 10 * time.Millisecond
	for {
		status, op := sm.px.Status(seq)
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

func (sm *ShardMaster) make_agreement(op Op) (paxos.Fate, Config) {
	seq := sm.px.Max() + 1
	sm.px.Start(seq, op)
	for status, v := sm.check_status(seq); v.UUID != op.UUID; status, v = sm.check_status(seq) {
		if status != paxos.Decided {
			// failed to reach decision
			return status, Config{}
		}
		seq++
		sm.px.Start(seq, op)
	}
	return paxos.Decided, Config{}
}

func (sm *ShardMaster) latest_config() Config {
	return sm.configs[len(sm.configs)-1]
}

func get_config_copy(target Config) Config {
	new_group := make(map[int64][]string)
	for key, value := range target.Groups {
		new_group[key] = value
	}
	new_config := Config{
		Num:    target.Num,
		Shards: target.Shards,
		Groups: new_group,
	}
	return new_config
}

// get the gids of group holding min and max number of shards
func get_min_max_gid(config *Config) (int64, int64) {
	min_gid, max_gid := int64(0), int64(0)
	counts := make(map[int64]int)
	max, min := -1, 1000
	for _, group := range config.Shards {
		counts[group] += 1
		if counts[group] > max {
			max = counts[group]
			max_gid = group
		} else if counts[group] < min {
			min = counts[group]
			min_gid = group
		}
	}
	return min_gid, max_gid
}

func get_shards_by_gid(config *Config) {

}

func (sm *ShardMaster) rebalance(gid int64, servers []string, op string) Config {
	new_config := get_config_copy(sm.latest_config())
	new_config.Num += 1
	for i := 0; ; i++ {
		if op == JOIN {
			new_config.
			if i == NShards/len(new_config.Groups) {
				break
			}
		}
	}
	return new_config
}

/*=====================Interfaces===================*/

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
}

type Op struct {
	UUID    int64
	Gid     int64
	Action  string
	Shard   int
	Num     int
	Servers []string
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	new_gid := args.GID
	servers := args.Servers
	latest_config := sm.latest_config()
	if _, exist := latest_config.Groups[new_gid]; !exist {
		sm.make_agreement(Op{
			UUID:    nrand(),
			Gid:     new_gid,
			Action:  JOIN,
			Servers: servers,
		})
	}
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	target := args.GID
	shard := args.Shard
	new_config := get_config_copy(sm.latest_config())
	new_config.Num += 1
	new_config.Shards[shard] = target
	op := Op{
		UUID:   nrand(),
		Gid:    target,
		Action: MOVE,
		Shard:  shard,
	}
	fate, _ := sm.make_agreement(op)
	if fate != paxos.Decided {
		DPrintf("[Move] [Server %d] [Config %d] Failed to reach agreement\n", sm.me, new_config.Num)
	} else {
		sm.configs = append(sm.configs, new_config)
	}
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	target := args.Num
	if target == -1 {
		reply.Config = sm.latest_config()
	} else {
		reply.Config = sm.configs[target]
	}
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
