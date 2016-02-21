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

const SUCCESS = "SUCCESS"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32  // for testing
	unreliable int32  // for testing
	me         string //server's name (host:port)
	vs         *viewservice.Clerk
	view       viewservice.View
	db         map[string]string
	// Your declarations here.
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	reply.Value = pb.db[args.Key]
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	switch args.Op {
	case "Put":
		pb.db[args.Key] = args.Value
		reply.Err = SUCCESS
	case "Append":
		if _, ok := pb.db[args.Key]; ok {
			pb.db[args.Key] += args.Value
		} else {
			//key not exists in db, return no key error
			pb.db[args.Key] = args.Value
		}
		reply.Err = SUCCESS
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
	latestView, ok := pb.vs.Get()
	//fmt.Printf("latestView Primary: %s, view num: %d; PB me: %s, view num: %d, Primary: %s\n",
	//latestView.Primary, latestView.Viewnum, pb.me, pb.view.Viewnum, pb.view.Primary)
	if ok == false {
		fmt.Errorf("%s", "Error: Can't get lastest view from viewserver")
	}
	//if viewserver doesn't have a primary, ping it to establish
	if latestView.Primary == "" {
		fmt.Println("Pinging viewservice")
		pb.view, _ = pb.vs.Ping(pb.view.Viewnum)
	}
	pb.vs.Ping(pb.view.Viewnum)
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
