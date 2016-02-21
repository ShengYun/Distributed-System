package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32  // for testing
	unreliable int32  // for testing
	me         string //server's name (host:port)
	vs         *viewservice.Clerk
	view       viewservice.View
	db         map[string]string
	synced     bool //flag to indicate if have already synced with backup
	// Your declarations here.
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	if pb.view.Primary == pb.me {
		reply.Value = pb.db[args.Key]
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrWrongServer
	}
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	ok := true
	if pb.view.Backup != "" {
		ok = call(pb.view.Backup, "PBServer.PutAppend", args, &reply)
	}
	switch args.Op {
	case "Put":
		pb.db[args.Key] = args.Value
		if ok == false {
			fmt.Printf("Error:Put() RPC call to Backup %s failed\n", pb.view.Backup)
		}
		reply.Err = OK
	case "Append":
		if _, exist := pb.db[args.Key]; exist {
			pb.db[args.Key] += args.Value
		} else {
			//key not exists in db
			pb.db[args.Key] = args.Value
		}
		if ok == false {
			fmt.Printf("Error:Append() RPC call to Backup %s failed\n", pb.view.Backup)
		}
	}
	reply.Err = OK
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	if pb.view.Primary == "" {
		pb.view, _ = pb.vs.Ping(0)
		pb.vs.Ping(pb.view.Viewnum)
	} else {
		fmt.Printf("Server %s pinging\n", pb.me)
		latestView, _ := pb.vs.Ping(pb.view.Viewnum)
		if latestView.Viewnum != pb.view.Viewnum {
			//fmt.Printf("Updating %s from view %d to view %d\n", pb.me, pb.view.Viewnum, latestView.Viewnum)
			pb.view = latestView
		}
		pb.vs.Ping(pb.view.Viewnum)
		if pb.me == pb.view.Primary && pb.synced == false && pb.view.Backup != "" {
			fmt.Println("Primer sending sync")
			pb.sendSync()
		}
	}
}

func (pb *PBServer) sendSync() {
	args := &SyncArgs{}
	args.Db = pb.db
	var reply SyncReply
	ok := call(pb.view.Backup, "PBServer.Sync", args, &reply)
	if ok == false {
		fmt.Printf("Error: Can't Sync with Backup\n")
	}
	pb.synced = true
}

func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
	if pb.view.Backup == pb.me {
		pb.db = args.Db
		reply.Err = OK
	}
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.view = viewservice.View{0, me, "", false}
	pb.db = make(map[string]string)
	pb.synced = false
	// Your pb.* initializations here.

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
