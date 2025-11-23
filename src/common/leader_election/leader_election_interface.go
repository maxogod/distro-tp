package leader_election

import "github.com/maxogod/distro-tp/src/common/models/protocol"

// UpdateCallbacks is a wrapper for nullable callbacks
type UpdateCallbacks struct {

	// ResetUpdates removes all old updates
	ResetUpdates func()

	// GetUpdates saves data coming out of the channel
	GetUpdates func(receivingCh chan *protocol.DataEnvelope)

	// SendUpdates takes as reference all the data received up to this function call
	// and puts all the data in the channel and closes it, or until DONE is received from the doneCh
	SendUpdates func(sendingCh chan *protocol.DataEnvelope, done chan bool)
}

// LeaderElection interface defines the methods required for implementing
// a leader election mechanism among distributed workers.
type LeaderElection interface {
	Start() error
	IsLeader() bool
	FinishClient(clientID string) error
	Close() error
}
