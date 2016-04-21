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
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
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

func remove_element(list *[]int64, target int64) {
	for i, e := range *list {
		if e == target {
			*list = append((*list)[:i], (*list)[i+1:]...)
		}
	}
}

func get_min_max_gid(c *Config) (int64, int64) {
	min_id, min_num, max_id, max_num := int64(0), 999, int64(0), -1
	counts := map[int64]int{}
	for g := range c.Groups {
		counts[g] = 0
	}
	for _, g := range c.Shards {
		counts[g]++
	}
	for g := range counts {
		_, exists := c.Groups[g]
		if exists && min_num > counts[g] {
			min_id, min_num = g, counts[g]
		}
		if exists && max_num < counts[g] {
			max_id, max_num = g, counts[g]
		}
	}
	for _, g := range c.Shards {
		if g == 0 {
			max_id = 0
		}
	}
	return min_id, max_id
}

func get_shard_by_gid(gid int64, c *Config) int {
	for s, g := range c.Shards {
		if g == gid {
			return s
		}
	}
	return -1
}

//rebalancing the workload of shards among different groups
func (sm *ShardMaster) rebalance(group int64, op string, c *Config) {
	for i := 0; ; i++ {
		min_id, max_id := get_min_max_gid(c)
		if op == LEAVE {
			s := get_shard_by_gid(group, c)
			if s == -1 {
				break
			}
			c.Shards[s] = min_id
		} else {
			if i == NShards/len(c.Groups) {
				break
			}
			s := get_shard_by_gid(max_id, c)
			c.Shards[s] = group
		}
	}
}

func (sm *ShardMaster) apply(op Op) Config {
	sm.processed++
	new_config := get_config_copy(sm.latest_config())
	new_config.Num += 1
	switch op.Action {
	case JOIN:
		DPrintf("[Join] [Master %d] [Servers: %v -> GID: %d]\n", sm.me, op.Servers, op.Gid)
		new_config.Groups[op.Gid] = op.Servers
		sm.rebalance(op.Gid, JOIN, &new_config)
	case LEAVE:
		DPrintf("[Leave] [Master %d] [GID: %d]\n", sm.me, op.Gid)
		delete(new_config.Groups, op.Gid)
		sm.rebalance(op.Gid, LEAVE, &new_config)
	case MOVE:
		new_config.Shards[op.Shard] = op.Gid
	case QUERY:
		if op.Num == -1 {
			return sm.latest_config()
		} else {
			return sm.configs[op.Num]
		}
	}
	sm.px.Done(sm.processed)
	sm.configs = append(sm.configs, new_config)
	return Config{}
}

func (sm *ShardMaster) do(op Op) Config {
	for true {
		seq := sm.processed + 1
		status, v := sm.px.Status(seq)
		var value Op
		if status == paxos.Decided {
			value = v.(Op)
		} else {
			sm.px.Start(seq, op)
			_, value = sm.check_status(seq)
		}
		config := sm.apply(value)
		if value.UUID == op.UUID {
			return config
		}
	}
	return Config{}
}

/*=====================Interfaces===================*/

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs   []Config // indexed by config num
	processed int      // processed seq
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
	sm.mu.Lock()
	defer sm.mu.Unlock()
	new_gid := args.GID
	servers := args.Servers
	latest_config := sm.latest_config()
	if _, exist := latest_config.Groups[new_gid]; !exist {
		sm.do(Op{
			UUID:    nrand(),
			Gid:     new_gid,
			Action:  JOIN,
			Servers: servers,
		})
	}
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	gid := args.GID
	//latest_config := sm.latest_config()
	//if _, exist := latest_config.Groups[gid]; exist {
	sm.do(Op{
		UUID:   nrand(),
		Gid:    gid,
		Action: LEAVE,
	})
	//}
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	target := args.GID
	shard := args.Shard
	sm.do(Op{
		UUID:   nrand(),
		Gid:    target,
		Action: MOVE,
		Shard:  shard,
	})
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	num := args.Num
	reply.Config = sm.do(Op{
		UUID:   nrand(),
		Num:    num,
		Action: QUERY,
	})
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
	sm.processed = 0

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
