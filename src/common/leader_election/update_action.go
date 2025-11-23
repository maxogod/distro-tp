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
	for envelope := range le.updatesCh {
		savingCh <- envelope
	}
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

func (le *leaderElection) handleUpdateMsg(payload []byte) {
	dataEnvelope := &protocol.DataEnvelope{}
	if err := proto.Unmarshal(payload, dataEnvelope); err != nil {
		logger.Logger.Warn("Received a bad data envelope in an update message")
		return
	}

	if dataEnvelope.GetIsDone() {
		close(le.updatesCh)
	} else {
		le.updatesCh <- dataEnvelope
	}
}

// startSendingUpdates should be run as a go routine and it will send the envelopes that gets
// from the send updates callback for a given nodeID.
func (le *leaderElection) startSendingUpdates(nodeID int32) {
	middleware, exists := le.connectedNodes[nodeID]
	if !exists {
		logger.Logger.Errorf("NodeID %d is not in connected map", nodeID)
		return
	}

	sendingCh := make(chan *protocol.DataEnvelope)
	le.updateCallbacks.SendUpdates(sendingCh)
	for envelope := range sendingCh {
		if !le.running.Load() {
			return
		}

		payload, err := proto.Marshal(envelope)
		if err != nil {
			logger.Logger.Warn("Couldnt marshal envelope when sending updates")
			continue
		}
		middleware.Send(payload)
	}
}
