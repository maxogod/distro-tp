package leader_election

import "github.com/maxogod/distro-tp/src/common/models/protocol"

// LeaderElection interface defines the methods required for implementing
// a leader election mechanism among distributed workers.
type LeaderElection interface {
	Start(resetUpdates func(), get_updates chan *protocol.DataEnvelope, send_updates func(chan *protocol.DataEnvelope)) error
	IsLeader() bool
	FinishClient(clientID string) error
	Close() error
}
