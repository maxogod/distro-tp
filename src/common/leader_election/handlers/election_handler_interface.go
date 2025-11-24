package handlers

type ElectionHandler interface {
	// StartElection initiates the election process.
	StartElection()
	// HandleElectionMessage processes an incoming election message from a node.
	HandleElectionMessage(nodeId int32)
	// HandleAckMessage processes an incoming acknowledgment message for a given round.
	HandleAckMessage(roundID uint64)
	// HandleCoordinatorMessage processes an incoming coordinator message for a given round.
	HandleCoordinatorMessage(roundID uint64)
	// Close cleans up resources used by the election handler.
	Close() error
}
