package handlers

import (
	"context"
	"sync/atomic"
	"time"

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

	roundID atomic.Uint64

	// Timeouts
	ackTimeout   time.Duration
	coordTimeout time.Duration

	// Channels for communication
	ackCh   chan uint64
	coordCh chan uint64

	ctx    context.Context
	cancel context.CancelFunc
}

// NewElectionHandler creates a new election handler instance
func NewElectionHandler(nodeId int32, connectedNodes map[int32]middleware.MessageMiddleware, coordMiddleware middleware.MessageMiddleware, ackTimeout, coordTimeout time.Duration) ElectionHandler {
	ctx, cancel := context.WithCancel(context.Background())
	eh := &electionHandler{
		id:              nodeId,
		connectedNodes:  connectedNodes,
		coordMiddleware: coordMiddleware,
		ackTimeout:      ackTimeout,
		coordTimeout:    coordTimeout,
		ackCh:           make(chan uint64),
		coordCh:         make(chan uint64),
		ctx:             ctx,
		cancel:          cancel,
	}
	return eh
}

func (eh *electionHandler) StartElection() {
	for nodeID, nodeConn := range eh.connectedNodes {
		if eh.id > nodeID {
			continue
		}
		eh.sendElectionMessage(nodeConn)
	}
	go eh.runElectionTimeout()
}

func (eh *electionHandler) HandleElectionMessage(nodeId int32) {
	nodeConn, ok := eh.connectedNodes[nodeId]
	if !ok {
		logger.Logger.Errorf("No connection found for node %d to send ACK", nodeId)
		return
	}
	eh.sendAckMessage(nodeConn)

	foundHigher := false
	for id, conn := range eh.connectedNodes {
		if eh.id > id {
			continue
		}
		foundHigher = true
		eh.sendElectionMessage(conn)
	}

	if !foundHigher {
		eh.sendCoordinatorMessage()
		logger.Logger.Infof("Node %d became coordinator", eh.id)
		return
	}

	go eh.runElectionTimeout()
}

func (eh *electionHandler) Close() error {
	if eh.cancel != nil {
		eh.cancel()
		close(eh.ackCh)
		close(eh.coordCh)
	}
	return nil
}

func (eh *electionHandler) HandleAckMessage(roundID uint64) {
	select {
	case eh.ackCh <- roundID:
	default:
	}
}

func (eh *electionHandler) HandleCoordinatorMessage(roundID uint64) {
	select {
	case eh.coordCh <- roundID:
	default:
	}
}

/* -------- Private Methods -------- */

func (eh *electionHandler) runElectionTimeout() {
	roundID := eh.roundID.Add(1)
	timer := time.NewTimer(eh.ackTimeout)

	for {
		select {
		case <-eh.ctx.Done():
			timer.Stop()
			return
		case r := <-eh.ackCh:
			if r == roundID {
				timer.Stop()
				eh.awaitCoordinator(roundID)
				return
			}

		case <-timer.C:
			logger.Logger.Infof("ACK timeout expired on node %d; no higher nodes responded, becoming leader", eh.id)
			eh.sendCoordinatorMessage()
			logger.Logger.Infof("Node %d became coordinator", eh.id)
			return
		}

	}
}

/* -------- Await Coordinator Mode -------- */

func (eh *electionHandler) awaitCoordinator(roundID uint64) {
	timer := time.NewTimer(eh.coordTimeout)
	defer timer.Stop()

	for {
		select {
		case <-eh.ctx.Done():
			return
		case <-timer.C:
			logger.Logger.Infof("Coordinator timeout expired on node %d; no coordinator message received", eh.id)
			eh.StartElection()
			return
		case r := <-eh.coordCh:
			if r == roundID {
				logger.Logger.Debugf("Node %d received COORDINATOR for round %d", eh.id, roundID)
				return // Election complete, someone else is leader
			}
		}
	}
}

/* -------- Send functions -------- */

func (eh *electionHandler) sendElectionMessage(nodeConn middleware.MessageMiddleware) {
	electionMsg := &protocol.SyncMessage{
		NodeId: int32(eh.id),
		Action: int32(enum.ELECTION),
	}
	payload, err := proto.Marshal(electionMsg)
	if err != nil {
		logger.Logger.Errorf("Failed to marshal ELECTION message: %v", err)
		return
	}
	nodeConn.Send(payload)
}

func (eh *electionHandler) sendAckMessage(nodeConn middleware.MessageMiddleware) {
	ackMsg := &protocol.SyncMessage{
		NodeId:  int32(eh.id),
		Action:  int32(enum.ACK),
		RoundId: eh.roundID.Load(),
	}
	payload, err := proto.Marshal(ackMsg)
	if err != nil {
		logger.Logger.Errorf("Failed to marshal ACK message: %v", err)
		return
	}
	nodeConn.Send(payload)
}

func (eh *electionHandler) sendCoordinatorMessage() {
	coordMsg := &protocol.SyncMessage{
		NodeId:  int32(eh.id),
		Action:  int32(enum.COORDINATOR),
		RoundId: eh.roundID.Load(),
	}
	payload, err := proto.Marshal(coordMsg)
	if err != nil {
		logger.Logger.Errorf("Failed to marshal COORDINATOR message: %v", err)
		return
	}

	eh.coordMiddleware.Send(payload)
}
