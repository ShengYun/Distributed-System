package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "time"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key   string // value of this op instance
	Value string // key this op instance affects
	JID   int64
	Op    string // operation type (get,put,append)
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	log        map[int64]bool
	db         map[string]string
}

// interface for Paxos

//px = paxos.Make(peers []string, me int)

//px.Start(seq int, v interface{}) --> start agreement on new instance

//px.Status(seq int) (fate Fate, v interface{}) --> get info about an instance

//px.Done(seq int) --> ok to forget all instances <= seq

//px.Max() int --> highest instance seq known, or -1

//px.Min() int --> instances before this have been forgotten

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	if _, exist := kv.log[args.JID]; !exist {
		kv.log[args.JID] = true
		kv.mu.Unlock()
		op := Op{
			Key:   args.Key,
			Value: "",
			JID:   args.JID,
			Op:    "Get",
		}
		seq := kv.px.Max() + 1
		ok := kv.makeAgreement(seq, op)
		if ok == false {
			// current server not in majority, can't serve client request
			reply.Err = Timeout
		} else {
			value, err := kv.updateDbAndGetValue(args.Key)
			if err == OK {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
		}
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	if _, exist := kv.log[args.JID]; !exist {
		kv.log[args.JID] = true
		kv.mu.Unlock()
		op := Op{
			Key:   args.Key,
			Value: args.Value,
			JID:   args.JID,
			Op:    args.Op,
		}
		seq := kv.px.Max() + 1
		ok := kv.makeAgreement(seq, op)
		if ok == false {
			reply.Err = Timeout
		} else {
			reply.Err = OK
		}
	}
	return nil
}

func (kv *KVPaxos) makeAgreement(seq int, op Op) bool {
	// try increasing seq number until current JID have been logged
	kv.px.Start(seq, op)
	for status, v := kv.checkStatus(seq); v.JID != op.JID; {
		if status != paxos.Decided {
			return false
		}
		seq++
		kv.px.Start(seq, op)
	}
	return true
}

func (kv *KVPaxos) updateDbAndGetValue(key string) (string, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// applying all existing seq's change to db
	start := kv.px.Min()
	end := kv.px.Max()
	for seq := start; seq <= end; seq++ {
		status, op := kv.px.Status(seq)
		if status == paxos.Decided {
			switch op.(Op).Op {
			case "Put":
				kv.db[op.(Op).Key] = op.(Op).Value
			case "Append":
				kv.db[op.(Op).Key] += op.(Op).Value
			}
		}
	}
	// call Done() to release memory
	kv.px.Done(end)
	if value, exist := kv.db[key]; !exist {
		return "", ErrNoKey
	} else {
		return value, OK
	}
}

func (kv *KVPaxos) checkStatus(seq int) (paxos.Fate, Op) {
	to := 10 * time.Millisecond
	for {
		status, op := kv.px.Status(seq)
		if status == paxos.Decided {
			return paxos.Decided, op.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		} else {
			return paxos.Pending, op.(Op)
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.log = make(map[int64]bool)
	kv.db = make(map[string]string)

	// Your initialization code here.

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
