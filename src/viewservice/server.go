package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

//self made set structure to
//avoid duplicates in idle server
//list
type addrSet struct {
	set map[string]bool
}

func (set *addrSet) insert(address string) {
	set.set[address] = true
}

func (set *addrSet) remove(address string) {
	delete(set.set, address)
}

func (set *addrSet) getOne() string {
	var address string
	for addr, _ := range set.set {
		address = addr
		break
	}
	return address
}

func (set *addrSet) Len() int {
	return len(set.set)
}

type ViewServer struct {
	mu           sync.Mutex
	l            net.Listener
	dead         int32          // for testing
	rpccount     int32          // for testing
	me           string
	serverStates map[string]Log //server status logs,{address:Log}
	idleServers  addrSet
	currentView  View
}

type Log struct {
	address  string
	viewnum  uint
	lastPing time.Time
}

// server Ping RPC handler.
// The PingArgs contains the Pinging Server's
// name caller's notion of current viewnum
// should have a map of current servers' states
// key: each server's host:port
// value: Log
// Log struct contains server's address and it's latest ping timestamp.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	address, viewNum := args.Me, args.Viewnum
	//initialize primary or backup or mark server as idle
	if viewNum == 0 {
		if address == vs.currentView.Primary {
			//current primary restarted, treated as dead
			fmt.Printf("Detected Primary server %s restart, changing view\n", vs.currentView.Primary)
			vs.changeView(vs.currentView.Backup, vs.getBackup())
		} else {
			//fmt.Printf("Got new server %s ping\n", address)
			switch "" {
			case vs.currentView.Primary:
				vs.currentView.Primary = address
				vs.currentView.Ack = false
				vs.currentView.Viewnum++
			//fmt.Printf("Current Primary %s\n", vs.currentView.Primary)
			case vs.currentView.Backup:
				vs.changeView(vs.currentView.Primary, address)
			default:
				//fmt.Printf("Adding server %s to idle list\n", address)
				if address != vs.currentView.Primary && address != vs.currentView.Backup {
					vs.idleServers.insert(address)
				}
			}
		}
	}
	//received Ack from primary
	if viewNum == vs.currentView.Viewnum && address == vs.currentView.Primary {
		//fmt.Printf("Current Primary %s acked view %d\n", vs.currentView.Primary, vs.currentView.Viewnum)
		vs.currentView.Ack = true
	}
	reply.View = vs.currentView
	vs.serverStates[address] = Log{address, viewNum, time.Now()}
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	reply.View = vs.currentView
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	for server, log := range vs.serverStates {
		now := time.Now()
		//the server is considered dead and need to be replaced
		//because of no pings for a while or reboot
		if now.Sub(log.lastPing) / PingInterval > DeadPings {
			//fmt.Printf("Client %s with view %d is dead, current Primary %s, current Backup %s\n", log.address, vs.currentView.Viewnum, vs.currentView.Primary, vs.currentView.Backup)
			switch server {
			case vs.currentView.Primary:
				//promote current backup to primary and
				//find a new backup, issue a go routin to
				//change view
				//fmt.Printf("Primary %s failed, current Backup %s with viewnum %d\n", server, vs.currentView.Backup, vs.serverStates[vs.currentView.Backup].viewnum)
				//only initialized backup got to be promoted
				if vs.serverStates[vs.currentView.Backup].viewnum != 0 {
					vs.changeView(vs.currentView.Backup, vs.getBackup())
				}
			case vs.currentView.Backup:
				//find a new backup, and issue a go routine to change view
				vs.changeView(vs.currentView.Primary, vs.getBackup())
			default:
				//if the dead client is in idle server list
				//remove it from list
				vs.idleServers.remove(server)
			}

			delete(vs.serverStates, server)
		}
	}
}

//wait for the current view to be acked by current primary then proceed
//to change current view
func (vs *ViewServer) changeView(primary string, backup string) {
	//current view is not acked by primary
	//can't proceed
	//fmt.Printf("Wait for current view %d to be acked\n", vs.currentView.Viewnum)
	for i := 0; i < DeadPings * 2; i++ {
		if vs.currentView.Ack == true {
			fmt.Printf("Current view %d acked\n", vs.currentView.Viewnum)
			//current view acked, proceed
			vs.currentView.Ack = false
			vs.currentView.Primary = primary
			vs.currentView.Backup = backup
			vs.currentView.Viewnum++
			fmt.Printf("View change complete, current view %d, Primary %s, Backup %s\n", vs.currentView.Viewnum, vs.currentView.Primary, vs.currentView.Backup)
			break
		}
		time.Sleep(PingInterval)
	}

}

func (vs *ViewServer) getBackup() string {
	if vs.idleServers.Len() > 0 {
		backup := vs.idleServers.getOne()
		vs.idleServers.remove(backup)
		//fmt.Printf("Returned backup %s, next idle server %s\n", backup, vs.idleServers.getOne())
		return backup
	} else {
		//fmt.Println("No idle server to be backup")
		return ""
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.serverStates = make(map[string]Log)
	vs.idleServers.set = make(map[string]bool)
	// Your vs.* initializations here.

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
