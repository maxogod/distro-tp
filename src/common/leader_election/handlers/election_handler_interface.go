package handlers

// ElectionHandler defines handling methods for election related operations.
type ElectionHandler interface {
	// StartElection initiates the election process.
	StartElection()

	// StopElection halts any ongoing election process.
	// Should be called when a leader has been elected.
	StopElection()

	// HandleElectionMessage processes an incoming election message from a node.
	HandleElectionMessage(nodeId int32, roundID string)

	// HandleAckMessage processes an incoming acknowledgment message for a given round.
	HandleAckMessage(roundID string)
}
