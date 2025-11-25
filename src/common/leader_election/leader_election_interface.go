package leader_election

import "github.com/maxogod/distro-tp/src/common/models/protocol"

// UpdateCallbacks is a wrapper for nullable callbacks.
type UpdateCallbacks interface {
	// ResetUpdates is called when the leader wants to reset the state of updates.
	ResetUpdates()
	// GetUpdates is called to receive updates from other nodes.
	GetUpdates(receivingCh chan *protocol.DataEnvelope)
	// SendUpdates is called to send updates to other nodes.
	SendUpdates(sendingCh chan *protocol.DataEnvelope, done chan bool)
}

// LeaderElection interface defines the methods required for implementing
// a leader election mechanism among distributed workers.
type LeaderElection interface {

	// Start should be run in a go routine and initiates the leader election algorithm.
	Start() error

	// IsLeader returns whether the node is leader in a atomic fashion.
	IsLeader() bool

	// FinishClient notifies other nodes the finishing of a given clientID to
	// synchronize data.
	FinishClient(clientID string) error

	// Close releases any used resources.
	Close() error
}
