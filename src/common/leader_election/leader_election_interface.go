package leader_election

import "github.com/maxogod/distro-tp/src/common/models/protocol"

// UpdateCallbacks is a wrapper for nullable callbacks
type UpdateCallbacks struct {
	ResetUpdates func()
	GetUpdates   func(chan *protocol.DataEnvelope)
	SendUpdates  func(chan *protocol.DataEnvelope)
}

// LeaderElection interface defines the methods required for implementing
// a leader election mechanism among distributed workers.
type LeaderElection interface {
	Start() error
	IsLeader() bool
	FinishClient(clientID string) error
	Close() error
}
