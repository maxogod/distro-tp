package leader_election

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

// awaitUpdates blocks waiting and saving updates until DONE message is received or server is closed.
// It has a timeout after which it assumes no more updates are comming, likely the leader fell.
func (le *leaderElection) awaitUpdates() {
	le.updateCallbacks.ResetUpdates()

	le.sendRequestUpdate()

	// TODO: timeout
	savingCh := make(chan *protocol.DataEnvelope)
	go le.updateCallbacks.GetUpdates(savingCh)
	for envelope := range le.updatesCh {
		if envelope.GetIsDone() {
			break
		}
		savingCh <- envelope
	}
	close(savingCh)
}

// sendRequestUpdate constructs and sends the leader a request for updates.
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

// handleUpdateMsg handles the receiving of update payload messages.
func (le *leaderElection) handleUpdateMsg(payload []byte) {
	dataEnvelope := &protocol.DataEnvelope{}
	if err := proto.Unmarshal(payload, dataEnvelope); err != nil {
		logger.Logger.Warn("Received a bad data envelope in an update message")
		return
	}
	le.updatesCh <- dataEnvelope // updateCh closed at le.Close()
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
	go le.updateCallbacks.SendUpdates(sendingCh, le.routineShutdownCh)
	for envelope := range sendingCh { // Finishes when sending stops or routine shutdown
		payload, err := proto.Marshal(envelope)
		if err != nil {
			logger.Logger.Warn("Couldnt marshal envelope when sending updates")
			continue
		}
		middleware.Send(payload)
	}
}
