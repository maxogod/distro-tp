package leader_election

import "github.com/maxogod/distro-tp/src/common/models/protocol"

// UpdateCallbacks is a wrapper for nullable callbacks.
type UpdateCallbacks struct {

	// ResetUpdates removes all old updates.
	ResetUpdates func()

	// GetUpdates saves data coming out of the channel.
	GetUpdates func(receivingCh chan *protocol.DataEnvelope)

	// SendUpdates takes as reference all the data received up to this function call
	// and puts all the data in the channel and closes it, or until DONE is received from the doneCh.
	SendUpdates func(sendingCh chan *protocol.DataEnvelope, done chan bool)
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
