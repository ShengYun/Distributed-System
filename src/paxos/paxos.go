package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"

//import "math"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

const Debug = 0

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int              // index into peers[]
	log        map[int]Instance // each seq is mapped to an Instance hold info about it
	dones      map[int]int      // map of each peer's done value
}

type Instance struct {
	HighestPrep ProposalId
	HighestAC   ProposalId
	HighestACV  interface{}
	Value       interface{}
	Status      Fate
}

type ProposalId struct {
	Proposal int
	Who      int
}

func (pi *ProposalId) Greater(other *ProposalId) bool {
	return pi.Proposal > other.Proposal ||
		(pi.Proposal == other.Proposal && pi.Who > other.Who)
}

func (pi *ProposalId) Equal(other *ProposalId) bool {
	return pi.Proposal == other.Proposal && pi.Who == other.Who
}

func (pi *ProposalId) Geq(other *ProposalId) bool {
	return pi.Greater(other) || pi.Equal(other)
}

func NullProposal() ProposalId {
	return ProposalId{-1, -1}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	if seq < px.Min() {
		return
	}
	go px.makeProposal(seq, v)
}

func (px *Paxos) makeProposal(seq int, v interface{}) {
	fate, _ := px.Status(seq)
	if fate == Forgotten {
		return
	}
	// instance for proposal
	px.mu.Lock()
	var pInstance Instance
	if _, exist := px.log[seq]; !exist {
		px.log[seq] = Instance{
			HighestPrep: ProposalId{0, px.me},
			HighestAC:   NullProposal(),
			HighestACV:  nil,
			Status:      Pending,
			Value:       v}
	}
	pInstance = px.log[seq]
	proposal := pInstance.HighestPrep
	px.mu.Unlock()
	for fate != Decided && !px.isdead() {
		ac := 0 //accept count for prepare and accept
		value := v
		decided := false
		proposal.Proposal++
		proposal.Who = px.me
		//send prepare
		ac, decided, value = px.makePrepare(seq, proposal, value)
		DPrintf("[Seq %d] [**Prepare**] AC count : %d/%d for proposer %d proposal %v\n", seq, ac, len(px.peers), px.me, proposal)
		if ac > len(px.peers)/2 && !decided {
			ac, decided, value = px.makeAccept(seq, proposal, value)
		}
		if decided {
			DPrintf("[Seq %d] current seq is already decide with value %v (current proposer %d informed)\n", seq, value, px.me)
			px.makeDecide(seq, value)
		} else if ac > len(px.peers)/2 {
			DPrintf("[Seq %d] [**Accept**] AC count : %d/%d for proposer %d proposal %v value %v\n", seq, ac, len(px.peers), px.me, proposal, value)
			px.makeDecide(seq, value)
		}
		fate, _ = px.Status(seq)
	}
}

func (px *Paxos) makePrepare(seq int, proposal ProposalId, v interface{}) (int, bool, interface{}) {
	count := 0
	highestResponse := proposal
	value := v
	for peerId, peer := range px.peers {
		ok := false
		args := &PrepareArgs{}
		args.Proposal = proposal
		args.Seq = seq
		var reply PrepareReply
		if peer == px.peers[px.me] {
			ok = true
			px.Prepare(args, &reply)
		} else {
			ok = call(peer, "Paxos.Prepare", args, &reply)
		}
		if ok == false || reply.Ok == "" {
			DPrintf("[Seq %d] Error: Prepare call from  %d to %d failed\n", seq, px.me, peerId)
		} else if reply.Ok == ACCEPTED {
			count++
			if reply.HighestAC.Greater(&highestResponse) {
				highestResponse = reply.HighestAC
				value = reply.Value
			}
		} else if reply.Decided == true {
			return count, true, reply.Value
		}
	}
	return count, false, value
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	DPrintf("[Seq %d] %d got Prepare proposal %v\n", args.Seq, px.me, args.Proposal)
	if _, exist := px.log[args.Seq]; !exist {
		px.log[args.Seq] = Instance{
			HighestPrep: ProposalId{0, px.me},
			HighestAC:   NullProposal(),
			HighestACV:  nil,
			Value:       nil,
			Status:      Pending}
	}
	// current instance to seq
	cInstance := px.log[args.Seq]
	reply.Done = px.dones[px.me]
	if cInstance.Status == Decided {
		reply.Ok = REJECTED
		reply.Decided = true
		reply.Value = cInstance.Value
	} else if args.Proposal.Greater(&cInstance.HighestPrep) {
		cInstance.HighestPrep = args.Proposal
		px.log[args.Seq] = cInstance
		reply.Ok = ACCEPTED
		reply.HighestAC = cInstance.HighestAC
		reply.Value = cInstance.HighestACV
	} else {
		reply.Ok = REJECTED
	}
	return nil
}

func (px *Paxos) makeAccept(seq int, proposal ProposalId, value interface{}) (int, bool, interface{}) {
	count := 0
	for peerId, peer := range px.peers {
		args := &AcceptArgs{}
		args.Proposal = proposal
		args.Seq = seq
		args.Value = value
		var reply AcceptReply
		ok := true
		if peer == px.peers[px.me] {
			px.Accept(args, &reply)
		} else {
			ok = call(peer, "Paxos.Accept", args, &reply)
		}
		if ok == false || reply.Ok == "" {
			DPrintf("[Seq %d] Error: Accept call from %d to peer %d failed\n", seq, px.me, peerId)
		} else {
			px.dones[peerId] = reply.Done
			if reply.Ok == ACCEPTED {
				count++
			} else if reply.Decided == true {
				return count, true, reply.Value
			} else {
				DPrintf("[Seq %d] Accept Proposal %v got rejected by %d with HighestPrep: %v\n", seq, proposal, peerId, reply.HighestPrep)
			}
		}
	}
	return count, false, value
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	DPrintf("[Seq %d] %d got Accept proposal %v\n", args.Seq, px.me, args.Proposal)
	if _, exist := px.log[args.Seq]; !exist {
		px.log[args.Seq] = Instance{
			HighestPrep: ProposalId{0, px.me},
			HighestAC:   NullProposal(),
			HighestACV:  nil,
			Value:       nil,
			Status:      Pending}
	}
	cInstance := px.log[args.Seq]
	reply.Done = px.dones[px.me]
	if cInstance.Status == Decided {
		reply.Ok = REJECTED
		reply.Decided = true
		reply.Value = cInstance.Value
	} else if args.Proposal.Geq(&cInstance.HighestPrep) {
		cInstance.HighestPrep = args.Proposal
		cInstance.HighestAC = args.Proposal
		cInstance.HighestACV = args.Value
		px.log[args.Seq] = cInstance
		reply.Ok = ACCEPTED
		reply.HighestPrep = args.Proposal
	} else {
		reply.HighestPrep = cInstance.HighestPrep
		reply.Ok = REJECTED
	}
	return nil
}

func (px *Paxos) makeDecide(seq int, v interface{}) {
	for peerId, peer := range px.peers {
		args := &DecideArgs{}
		args.Seq = seq
		args.Value = v
		var reply DecidedReply
		ok := true
		if peer == px.peers[px.me] {
			px.Learn(args, &reply)
		} else {
			ok = call(peer, "Paxos.Learn", args, &reply)
		}
		if ok == false || reply.Ok == "" {
			fmt.Printf("[Seq %d] Error:%d Fail to inform %d about decision\n", seq, px.me, peerId)
		}
	}
}

func (px *Paxos) Learn(args *DecideArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if _, exist := px.log[args.Seq]; !exist {
		DPrintf("[Seq %d] %d learned about the decision, value: %v, creating instance...\n", args.Seq, px.me, args.Value)
		px.log[args.Seq] = Instance{
			HighestPrep: ProposalId{0, px.me},
			HighestAC:   NullProposal(),
			HighestACV:  nil,
			Value:       args.Value,
			Status:      Decided}
	} else if cInstance := px.log[args.Seq]; cInstance.Status != Decided {
		DPrintf("[Seq %d] %d learned about the decision, value: %v\n", args.Seq, px.me, args.Value)
		cInstance.Value = args.Value
		cInstance.Status = Decided
		px.log[args.Seq] = cInstance
	}
	reply.Ok = "Decided"
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq > px.dones[px.me] {
		px.dones[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	highest := -1
	for seq, _ := range px.log {
		if seq > highest {
			highest = seq
		}
	}
	if px.dones[px.me] > highest {
		highest = px.dones[px.me]
	}
	return highest
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.dones[px.me]
	for _, seq := range px.dones {
		if seq < min {
			min = seq
		}
	}

	for seq, _ := range px.log {
		if seq <= min {
			delete(px.log, seq)
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	if seq < px.Min() {
		return Forgotten, nil
	}
	return px.log[seq].Status, px.log[seq].Value
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.dones = make(map[int]int)
	px.dones[px.me] = -1
	px.log = make(map[int]Instance)

	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
