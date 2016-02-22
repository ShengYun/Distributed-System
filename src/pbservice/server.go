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
	PAJobDone  map[int64]bool
	GetJobDone map[int64]bool
	synced     string   //synced backup
	hasBackup  bool
                      // Your declarations here.
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	//fmt.Printf("Get request for %s, current Primary %s\n", pb.me, pb.view.Primary)
	if _ , done := pb.GetJobDone[args.JobID]; !done {
		if pb.view.Primary == pb.me {
			//fmt.Printf("Get request for %s, current Primary %s\n", pb.me, pb.view.Primary)
			if _ , exist := pb.db[args.Key]; exist{
				reply.Value = pb.db[args.Key]
				reply.Err = OK
			} else {
				reply.Value = ""
				reply.Err = ErrNoKey
			}
		} else {
			reply.Value = ""
			reply.Err = ErrWrongServer
		}
	}
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	ok := true
	if pb.hasBackup {
		ok = call(pb.view.Backup, "PBServer.PutAppend", args, &reply)
	}
	if _, done := pb.PAJobDone[args.JobID]; !done {
		switch args.Op {
		case "Put":
			pb.db[args.Key] = args.Value
			if ok == false {
				fmt.Printf("Error:Put() RPC call to Backup %s failed\n", pb.view.Backup)
			}
			pb.PAJobDone[args.JobID] = true
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
			pb.PAJobDone[args.JobID] = true
			reply.Err = OK
		}
	}

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.view.Primary == "" {
		pb.view, _ = pb.vs.Ping(0)
		pb.vs.Ping(pb.view.Viewnum)
	} else {
		//fmt.Printf("Server %s pinging\n", pb.me)
		latestView, _ := pb.vs.Ping(pb.view.Viewnum)
		if latestView.Viewnum != pb.view.Viewnum {
			//fmt.Printf("Updating %s from view %d to view %d\n", pb.me, pb.view.Viewnum, latestView.Viewnum)
			pb.view = latestView
		}
		pb.vs.Ping(pb.view.Viewnum)
		if pb.me == pb.view.Primary && pb.synced != pb.view.Backup && pb.view.Backup != "" {
			pb.sendSync()
		}
	}
}

func (pb *PBServer) sendSync() {
	fmt.Printf("P %s sending sync to B %s\n", pb.me, pb.view.Backup)
	args := &SyncArgs{}
	args.Db = pb.db
	var reply SyncReply
	ok := call(pb.view.Backup, "PBServer.Sync", args, &reply)
	if ok == false || reply.Err != OK {
		fmt.Printf("Error: Can't Sync with Backup\n")
	}
	pb.synced = pb.view.Backup
	pb.hasBackup = true
}

func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
	pb.tick()
	pb.mu.Lock()
	defer pb.mu.Unlock()
	fmt.Printf("Got sync at B %s, current view B %s\n",pb.me, pb.view.Backup)
	if pb.view.Backup == pb.me {
		fmt.Printf("%s Synced\n",pb.me)
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
	pb.PAJobDone = make(map[int64]bool)
	pb.GetJobDone = make(map[int64]bool)
	pb.synced = ""
	pb.hasBackup = false
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
				if pb.isunreliable() && (rand.Int63() % 1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63() % 1000) < 200 {
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
