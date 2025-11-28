package handlers

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

type electionHandler struct {
	id              int32
	connectedNodes  map[int32]middleware.MessageMiddleware
	coordMiddleware middleware.MessageMiddleware

	isElectionRunning atomic.Bool
	roundID           string

	// Timeouts
	ackTimeout   time.Duration
	coordTimeout time.Duration

	electionCtx    context.Context
	electionCancel context.CancelFunc

	awaitCoordinatorCtx    context.Context
	awaitCoordinatorCancel context.CancelFunc
}

// NewElectionHandler creates a new election handler instance
func NewElectionHandler(nodeId int32, connectedNodes map[int32]middleware.MessageMiddleware, coordMiddleware middleware.MessageMiddleware, ackTimeout, coordTimeout time.Duration) ElectionHandler {
	eh := &electionHandler{
		id:              nodeId,
		connectedNodes:  connectedNodes,
		coordMiddleware: coordMiddleware,
		ackTimeout:      ackTimeout,
		coordTimeout:    coordTimeout,
	}
	return eh
}

func (eh *electionHandler) StartElection() {
	logger.Logger.Infof("[Node %d] ELECTION START", eh.id)
	if eh.isElectionRunning.Load() {
		logger.Logger.Infof("[Node %d] election already running, ignoring start request", eh.id)
		return
	}
	eh.isElectionRunning.Store(true)

	eh.electionCtx, eh.electionCancel = context.WithCancel(context.Background())
	eh.roundID = uuid.New().String() // New unique roundID
	for nodeID, nodeConn := range eh.connectedNodes {
		if eh.id > nodeID {
			continue
		}
		logger.Logger.Infof("[Node %d] sending ELECTION to node %d", eh.id, nodeID)
		eh.sendElectionMessage(nodeConn)
	}
	go eh.runElectionTimeout()
}

func (eh *electionHandler) StopElection() {
	logger.Logger.Infof("[Node %d] ELECTION STOP", eh.id)
	if eh.electionCancel != nil {
		eh.electionCancel()
	}
	if eh.awaitCoordinatorCancel != nil {
		eh.awaitCoordinatorCancel()
	}
	eh.isElectionRunning.Store(false)
}

func (eh *electionHandler) IsElectionRunning() bool {
	return eh.isElectionRunning.Load()
}

func (eh *electionHandler) HandleElectionMessage(nodeId int32, roundID string) {
	logger.Logger.Infof("[Node %d] received ELECTION from node %d", eh.id, nodeId)
	nodeConn, ok := eh.connectedNodes[nodeId]
	if !ok {
		logger.Logger.Errorf("[Node %d] No connection found for node %d to send ACK", eh.id, nodeId)
		return
	}
	eh.sendAckMessage(nodeConn, roundID)
	eh.StartElection() // Start own election
}

func (eh *electionHandler) HandleAckMessage(roundID string) {
	if roundID != eh.roundID {
		logger.Logger.Debugf("[Node %d] Outdated roundID ack", eh.id)
		return
	}

	// Cancel election but still wait for coordinator until found or timeout
	if eh.electionCancel != nil {
		eh.electionCancel()
	}

	eh.awaitCoordinatorCtx, eh.awaitCoordinatorCancel = context.WithCancel(context.Background())
	go eh.awaitCoordinator()
}

/* -------- Private Methods -------- */

func (eh *electionHandler) runElectionTimeout() {
	timer := time.NewTimer(eh.ackTimeout)
	defer timer.Stop()

	select {
	case <-eh.electionCtx.Done():
	case t := <-timer.C:
		elapsed := fmt.Sprintf("%.2f", time.Since(t.Add(-eh.ackTimeout)).Seconds())
		logger.Logger.Infof("[Node %d] no one ACKed after %s seconds becoming coordinator", eh.id, elapsed)
		eh.sendCoordinatorMessage()
	}
}

/* -------- Await Coordinator Mode -------- */

func (eh *electionHandler) awaitCoordinator() {
	timer := time.NewTimer(eh.coordTimeout)
	defer timer.Stop()

	select {
	case <-eh.awaitCoordinatorCtx.Done():
	case t := <-timer.C:
		elapsed := fmt.Sprintf("%.2f", time.Since(t.Add(-eh.coordTimeout)).Seconds())
		logger.Logger.Infof("[Node %d] Coordinator timeout after %s seconds; no coordinator message received", eh.id, elapsed)
		eh.StartElection()
	}
}

/* -------- Send functions -------- */

func (eh *electionHandler) sendElectionMessage(nodeConn middleware.MessageMiddleware) {
	electionMsg := &protocol.SyncMessage{
		NodeId:  eh.id,
		Action:  int32(enum.ELECTION),
		RoundId: eh.roundID,
	}
	payload, err := proto.Marshal(electionMsg)
	if err != nil {
		logger.Logger.Errorf("[Node %d] Failed to marshal ELECTION message: %v", eh.id, err)
		return
	}
	nodeConn.Send(payload)
}

func (eh *electionHandler) sendAckMessage(nodeConn middleware.MessageMiddleware, roundID string) {
	ackMsg := &protocol.SyncMessage{
		NodeId:  int32(eh.id),
		Action:  int32(enum.ACK),
		RoundId: roundID,
	}
	payload, err := proto.Marshal(ackMsg)
	if err != nil {
		logger.Logger.Errorf("[Node %d] Failed to marshal ACK message: %v", eh.id, err)
		return
	}
	nodeConn.Send(payload)
}

func (eh *electionHandler) sendCoordinatorMessage() {
	coordMsg := &protocol.SyncMessage{
		NodeId:   int32(eh.id),
		LeaderId: int32(eh.id),
		Action:   int32(enum.COORDINATOR),
	}
	payload, err := proto.Marshal(coordMsg)
	if err != nil {
		logger.Logger.Errorf("[Node %d] Failed to marshal COORDINATOR message: %v", eh.id, err)
		return
	}

	eh.coordMiddleware.Send(payload)
}
