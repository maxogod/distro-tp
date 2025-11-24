package leader_election

import (
	"fmt"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

// awaitUpdates blocks waiting and saving updates until DONE message is received or server is closed.
// It has a timeout after which it assumes no more updates are comming, likely the leader fell (returns it as error).
func (le *leaderElection) awaitUpdates() error {
	if le.updateCallbacks == nil {
		logger.Logger.Debugf("Node %d has no update callbacks, skipping updates", le.id)
		return nil
	}

	logger.Logger.Infof("[Node %d] Requesting updates from leader %d", le.id, le.leaderId.Load())
	le.updateCallbacks.ResetUpdates()

	le.sendRequestUpdate()

	timer := time.NewTimer(COORDINATOR_TIMEOUT)
	defer timer.Stop()

	savingCh := make(chan *protocol.DataEnvelope, MAX_CHAN_BUFFER)
	defer close(savingCh)

	go le.updateCallbacks.GetUpdates(savingCh)

	for {
		var envelope *protocol.DataEnvelope
		select {
		case e, ok := <-le.updatesCh:
			envelope = e
			if !ok {
				return fmt.Errorf("Updates channel closed unexpectedly")
			}
		case <-timer.C:
			return fmt.Errorf("Leader did not finish sending updates on time")
		}

		if envelope.GetIsDone() {
			break
		}
		savingCh <- envelope

		// Reset timer
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(COORDINATOR_TIMEOUT)
	}

	logger.Logger.Infof("[Node %d] Finished receiving updates from leader %d", le.id, le.leaderId.Load())

	return nil
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
	if le.updateCallbacks == nil {
		logger.Logger.Debugf("Node %d has no update callbacks, skipping sending updates", le.id)
		return
	}

	middleware, exists := le.connectedNodes[nodeID]
	if !exists {
		logger.Logger.Errorf("NodeID %d is not in connected map", nodeID)
		return
	}

	sendingCh := make(chan *protocol.DataEnvelope, MAX_CHAN_BUFFER)
	doneCh := make(chan bool)
	go le.updateCallbacks.SendUpdates(sendingCh, doneCh)

	syncMsg := &protocol.SyncMessage{
		NodeId:   le.id,
		Action:   int32(enum.UPDATE),
		Envelope: nil,
	}

	sending := true
	for sending { // Finishes when sending stops or routine shutdown
		var envelope *protocol.DataEnvelope
		select {
		case e, ok := <-sendingCh:
			envelope = e
			if !ok {
				sending = false
				continue
			}
		case <-le.ctx.Done():
			sending = false
			continue
		}
		payload, err := proto.Marshal(envelope)
		if err != nil {
			logger.Logger.Warn("Couldnt marshal envelope when sending updates")
			continue
		}

		syncMsg.Envelope = payload
		payload, err = proto.Marshal(syncMsg)
		if err != nil {
			logger.Logger.Warn("Couldnt marshal sync message when sending updates")
			continue
		}
		middleware.Send(payload)
	}
	doneCh <- true
}
