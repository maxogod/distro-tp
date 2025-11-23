package leader_election

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

func (le *leaderElection) awaitUpdates() {
	le.updateCallbacks.ResetUpdates()

	le.sendRequestUpdate()

	savingCh := make(chan *protocol.DataEnvelope)
	le.updateCallbacks.GetUpdates(savingCh)
}

func (le *leaderElection) sendRequestUpdate() {
	msg := &protocol.SyncMessage{
		NodeId: le.id,
		Action: int32(enum.UPDATE),
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		logger.Logger.Errorf("Node %d failed to marshal request update message: %v", le.id, err)
	}

	m, exists := le.connectedNodes[le.leaderId.Load()]
	if !exists {
		logger.Logger.Errorf("Node %d is not connected to leader", le.id)
	}
	m.Send(payload)
}
