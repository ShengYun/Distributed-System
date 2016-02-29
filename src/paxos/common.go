package paxos

const (
	ACCEPTED = "Accepted"
	REJECTED = "Rejected"
)

type PrepareArgs struct {
	Proposal ProposalId
	Seq      int
}

type PrepareReply struct {
	Ok          string
	HighestPrep ProposalId
	HighestAC   ProposalId
	Decided     bool
	Value       interface{}
	Done        int
}

type AcceptArgs struct {
	Proposal ProposalId
	Value    interface{}
	Seq      int
}

type AcceptReply struct {
	Ok          string
	Decided     bool
	HighestPrep ProposalId
	Value       interface{}
	Done        int
}

type DecideArgs struct {
	Seq   int
	Value interface{}
}

type DecidedReply struct {
	Ok string
}
