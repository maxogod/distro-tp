package enum

type LeaderElectionAction int32

const (
	COORDINATOR LeaderElectionAction = iota
	ELECTION
	DISCOVER
	ACK
	UPDATE
)
