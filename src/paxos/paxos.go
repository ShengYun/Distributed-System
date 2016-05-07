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
import "math/rand"
import "time"

const debugging bool = false

func Printf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
func Debugf(format string, args ...interface{}) {
	if debugging {
		fmt := "DEBUG: " + format
		log.Printf(fmt, args...)
	}
}
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

type ProposalId struct {
	Proposal int
	Who int
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
	return ProposalId { -1, -1 }
}

type Agree struct {
	fate Fate
	highestProposal ProposalId
	highestAccept ProposalId
	val interface{}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	log map[int]*Agree
	mins []int
}

type PrepareArgs struct {
	Seq int
	Proposal ProposalId
	Me int
	Min int
}

type PrepareReply struct {
	Ok bool
	Num ProposalId // if ok, highest known accept seen, else
	        // highest proposal seen
	Value interface{} // if ok and highest known accept >= 0,
	                  // then the value of the highest accept, else empty
}

type AcceptArgs struct {
	Seq int
	Proposal ProposalId
	Value interface{}
	Me int
	Min int
}

type AcceptReply struct {
	Ok bool
}

type DecideArgs struct {
	Seq int
	Value interface{}
	Me int
	Min int
}

type DecideReply struct {
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

func (px *Paxos) GetEntry(seq int) *Agree {
	a, ok := px.log[seq]
	if !ok {
		a = &Agree { Pending, NullProposal(), NullProposal(), 0 }
		px.log[seq] = a
	}
	return a
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	Debugf("[%d]: Prepare (Seq: %v, Proposal: %v, Me: %v, Min: %v)\n",
		px.me, args.Seq, args.Proposal, args.Me, args.Min)
	a := px.GetEntry(args.Seq)

	if args.Proposal.Greater(&a.highestProposal) {
		a.highestProposal = args.Proposal
		reply.Ok = true
		reply.Num = a.highestAccept
		reply.Value = a.val
	} else {
		reply.Ok = false
		reply.Num = a.highestProposal
	}

	if px.mins[args.Me] < args.Min {
		px.mins[args.Me] = args.Min
	}
	px.Clean()
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	Debugf("[%d]: Accept (Seq: %v, Proposal: %v, Value: %v, Me: %v, Min: %v)\n",
		px.me, args.Seq, args.Proposal, args.Value, args.Me, args.Min)
	a := px.GetEntry(args.Seq)

	if args.Proposal.Geq(&a.highestProposal) {
		a.highestProposal = args.Proposal
		a.highestAccept = args.Proposal
		a.val = args.Value
		reply.Ok = true
	} else {
		reply.Ok = false
	}

	if px.mins[args.Me] < args.Min {
		px.mins[args.Me] = args.Min
	}
	px.Clean()
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	Debugf("[%d]: Decide (Seq: %v, Value: %v, Me: %v, Min: %v)\n",
		px.me, args.Seq, args.Value, args.Me, args.Min)
	a := px.GetEntry(args.Seq)
	a.fate = Decided
	a.val = args.Value

	if px.mins[args.Me] < args.Min {
		px.mins[args.Me] = args.Min
	}
	px.Clean()
	return nil
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		to := 10 * time.Millisecond
		for {
			// "the tester calls Kill() when it wants your
			// Paxos to shut down; Kill() sets
			// px.dead. You should call px.isdead() in any
			// loops you have that might run for a while,
			// and break out of the loop if px.isdead() is
			// true. It's particularly important to do
			// this any in any long-running threads you
			// create."
			if px.isdead() {
				return
			}
			proposal := ProposalId { 0, px.me }
			// If this instance is decided, we return
			px.mu.Lock()
			a, ok := px.log[seq]
			if ok {
				if a.fate == Decided {
					px.mu.Unlock()
					return
				}
				proposal.Proposal = a.highestProposal.Proposal + 1
			}
			min := px.mins[px.me]
			px.mu.Unlock()
			// We send out prepares to all peers
			// (concurrently) and feed the replies back
			// into this channel
			type PrepareResult struct {
				ok bool
				reply *PrepareReply
			}
			c := make(chan PrepareResult, len(px.peers))
			args := &PrepareArgs{seq, proposal, px.me, min}
			for i := 0; i < len(px.peers); i++ {
				ind := i
				go func() {
					var reply PrepareReply
					ok := true
					if (ind == px.me) {
						err := px.Prepare(args, &reply)
						if err != nil {
							fmt.Println(err)
							ok = false
						}
					} else {
						ok = call(px.peers[ind], "Paxos.Prepare", args, &reply)
					}
					c <- PrepareResult {ok, &reply}
				}()
			}

			// Aggregate all the responses
			prepare_ok := 0
			highest_proposal := NullProposal()
			highest_accept := NullProposal()
			val := v
			for i := 0; i < len(px.peers) && prepare_ok <= (len(px.peers) / 2); i++ {
				p := <- c
				// if the request succeeded
				if p.ok {
					// if the proposal was accepted
					if p.reply.Ok {
						prepare_ok++
						if p.reply.Num.Greater(&highest_accept) {
							// record the highest accept
							highest_accept = p.reply.Num
							val = p.reply.Value
						}
					} else if p.reply.Num.Greater(&highest_proposal) {
						// a reject tells us the num
						highest_proposal = p.reply.Num
					}
				}
			}

			if prepare_ok > (len(px.peers) / 2) {
				// Send out the accepts
				type AcceptResult struct {
					ok bool
					reply *AcceptReply
				}
				c := make(chan AcceptResult, len(px.peers))
				args := &AcceptArgs{seq, proposal, val, px.me, min}
				for i := 0; i < len(px.peers); i++ {
					ind := i
					go func() {
						var reply AcceptReply
						ok := true
						if (ind == px.me) {
							err := px.Accept(args, &reply)
							if err != nil {
								fmt.Println(err)
								ok = false
							}
						} else {
							ok = call(px.peers[ind], "Paxos.Accept", args, &reply)
						}
						c <- AcceptResult { ok, &reply }
					}()
				}
				accepts := 0
				for i := 0; i < len(px.peers) && accepts <= (len(px.peers) / 2); i++ {
					a := <- c
					if a.ok && a.reply.Ok {
						accepts++
					}
				}

				if accepts > (len(px.peers) / 2) {
					args := &DecideArgs{seq, val, px.me, min}
					for i := 0; i < len(px.peers); i++ {
						ind := i
						var reply DecideReply
						if (ind == px.me) {
							px.Decide(args, &reply)
						} else {
							go func() {
								call(px.peers[ind], "Paxos.Decide", args, &reply)
							}()
						}
					}
				}
			} else {
				proposal.Proposal = highest_proposal.Proposal + 1
			}
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}

	}()
}

func (px *Paxos) Clean() {
	min := px.mins[0]
	for _, v := range px.mins {
		if v < min {
			min = v
		}
	}

	for k, _ := range px.log {
		if k <= min {
			delete(px.log, k)
		}
	}
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

	if (px.mins[px.me] < seq) {
		px.mins[px.me] = seq
	}

	px.Clean()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	seq := -1
	for k, _ := range px.log {
		if k > seq {
			seq = k
		}
	}
	return seq
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

	min := px.mins[0]
	for _, v := range px.mins {
		if v < min {
			min = v
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
	px.mu.Lock()
	defer px.mu.Unlock()

	a, ok := px.log[seq]
	if !ok {
		a = &Agree { Forgotten, NullProposal(), NullProposal(), 0 }
	}
	Debugf("[%d]: Status (Seq: %v) => (%v, %v)\n",
		px.me, seq, a.fate, a.val)
	return a.fate, a.val
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


	// Your initialization code here.
	px.log = make(map[int]*Agree)
	px.mins = make([]int, len(peers))

	for i := 0; i < len(px.mins); i++ {
		px.mins[i] = -1
	}

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
