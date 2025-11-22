package leader_election

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

type leader_election struct {
	id              int
	leaderId        int
	url             string
	workerType      enum.WorkerType
	coordMiddleware middleware.MessageMiddleware
	connMiddleware  middleware.MessageMiddleware
	nodeMiddleware  middleware.MessageMiddleware
	updateChan      chan *protocol.DataEnvelope
	connectedNodes  map[int]middleware.MessageMiddleware

	messagesCh chan *protocol.SyncMessage
	shutdownCh chan bool
	running    atomic.Bool

	round        uint64
	ackCh        chan uint64
	coordCh      chan uint64
	ackTimeout   time.Duration
	coordTimeout time.Duration
}

func NewLeaderElection(
	id int,
	middlewareUrl string,
	workerType enum.WorkerType,
) LeaderElection {
	le := &leader_election{
		id:              id,
		url:             middlewareUrl,
		workerType:      workerType,
		coordMiddleware: middleware.GetLeaderElectionCoordExchange(middlewareUrl, workerType),
		connMiddleware:  middleware.GetLeaderElectionDiscoveryExchange(middlewareUrl, workerType),
		nodeMiddleware:  middleware.GetLeaderElectionReceivingNodeExchange(middlewareUrl, workerType, strconv.Itoa(id)),

		updateChan:     make(chan *protocol.DataEnvelope),
		connectedNodes: make(map[int]middleware.MessageMiddleware),

		messagesCh: make(chan *protocol.SyncMessage),
		shutdownCh: make(chan bool),

		ackCh:   make(chan uint64, 16),
		coordCh: make(chan uint64, 16),

		ackTimeout:   2 * time.Second,
		coordTimeout: 5 * time.Second,
	}

	routineReadyCh := make(chan bool)
	go le.nodeQueueListener(routineReadyCh)
	<-routineReadyCh

	return le
}

func (le *leader_election) IsLeader() bool {
	return le.leaderId == le.id
}

func (le *leader_election) FinishClient(clientID string) error {
	if !le.IsLeader() {
		return nil
	}
	// Notify other nodes about client finish
	for _, connMiddleware := range le.connectedNodes {
		msg := protocol.DataEnvelope{
			IsDone:   true,
			ClientId: clientID,
		}

		envelopeBytes, err := proto.Marshal(&msg)
		if err != nil {
			return err
		}

		e := connMiddleware.Send(envelopeBytes)
		if e != middleware.MessageMiddlewareSuccess {
			return fmt.Errorf("failed to send finish message for client %s", clientID)
		}
	}
	return nil
}

func (le *leader_election) Start(
	resetUpdates func(),
	getUpdates chan *protocol.DataEnvelope,
	sendUpdates func(chan *protocol.DataEnvelope),
) error {
	le.running.Store(true)

	// reset any previous updates
	resetUpdates()

	// send that im alive to the world
	le.sendDiscoveryMessage()

	// begin recv data loop / heartbeat monitoring / election handleing
	for le.running.Load() {
		msg := <-le.messagesCh
		nodeID := int(msg.GetNodeId())
		switch msg.GetAction() {
		case int32(enum.DISCOVER):
			if _, exists := le.connectedNodes[nodeID]; !exists {
				le.connectedNodes[nodeID] = middleware.GetLeaderElectionSendingNodeExchange(le.url, le.workerType, strconv.Itoa(nodeID))
			}
		case int32(enum.COORDINATOR):
			le.leaderId = nodeID
			logger.Logger.Infof("Node %d recognized as coordinator", nodeID)
			select {
			case le.coordCh <- atomic.LoadUint64(&le.round):
			default:
			}
		case int32(enum.ELECTION):
			logger.Logger.Infof("Node %d received ELECTION from node %d", le.id, nodeID)
			le.handleElectionMsg(nodeID)
		case int32(enum.ACK):
			select {
			case le.ackCh <- atomic.LoadUint64(&le.round):
			default:
			}
		case int32(enum.REQUEST_UPDATES):
			if le.IsLeader() {
				// TODO: send updates to requesting node
			}
		default:
			logger.Logger.Warnf("Unknown leader election action received: %d", msg.GetAction())
		}
	}

	return nil
}

func (le *leader_election) Close() error {
	close(le.updateChan)
	le.running.Store(false)
	le.shutdownCh <- true

	if err := le.coordMiddleware.Close(); err != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error closing coord middleware: %d", int(err))
	}
	if err := le.connMiddleware.Close(); err != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error closing conn middleware: %d", int(err))
	}
	if err := le.nodeMiddleware.Close(); err != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error closing node middleware: %d", int(err))
	}

	for _, connMiddleware := range le.connectedNodes {
		if err := connMiddleware.Close(); err != middleware.MessageMiddlewareSuccess {
			logger.Logger.Errorf("error closing connected node middleware: %d", int(err))
		}
	}

	return nil
}

/* --- Private Methods --- */

func (le *leader_election) sendDiscoveryMessage() {
	discoveryMsg := &protocol.SyncMessage{
		NodeId: int32(le.id),
		Action: int32(enum.DISCOVER),
	}

	msgBytes, err := proto.Marshal(discoveryMsg)
	if err != nil {
		logger.Logger.Errorf("Failed to marshal discovery message: %v", err)
		return
	}

	e := le.connMiddleware.Send(msgBytes)
	if e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("Failed to send discovery message: %d", int(e))
	}
}

func (le *leader_election) nodeQueueListener(readyCh chan bool) {
	e := le.nodeMiddleware.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		readyCh <- true
		running := true
		for running {
			var msg middleware.MessageDelivery
			select {
			case m := <-consumeChannel:
				msg = m
			case <-le.shutdownCh:
				running = false
				continue
			}

			syncMessage := &protocol.SyncMessage{}
			err := proto.Unmarshal(msg.Body, syncMessage)
			if err != nil {
				logger.Logger.Errorf("Failed to unmarshal sync message: %v", err)
				return
			}
			le.messagesCh <- syncMessage

			msg.Ack(false)
		}
	})

	if e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("an error occurred while starting consumption: %d", int(e))
	}

}

func (le *leader_election) handleElectionMsg(nodeId int) {
	le.sendAckMessage(nodeId)

	electionMsg := &protocol.SyncMessage{
		NodeId: int32(le.id),
		Action: int32(enum.ELECTION),
	}

	electionBytes, err := proto.Marshal(electionMsg)
	if err != nil {
		logger.Logger.Errorf("Failed to marshal ELECTION message: %v", err)
		return
	}

	foundHigher := false
	higherIDs := make([]int, 0)
	for id := range le.connectedNodes {
		if id > le.id {
			foundHigher = true
			higherIDs = append(higherIDs, id)
		}
	}

	if !foundHigher {
		le.sendCoordinatorMessage()

		le.leaderId = le.id
		logger.Logger.Infof("Node %d became coordinator", le.id)
		return
	}

	for _, id := range higherIDs {
		if conn, ok := le.connectedNodes[id]; ok {
			if e := conn.Send(electionBytes); e != middleware.MessageMiddlewareSuccess {
				logger.Logger.Errorf("Failed to forward ELECTION to node %d: %d", id, int(e))
			}
		}
	}

	myRound := atomic.AddUint64(&le.round, 1)
	go le.runElectionTimeout(myRound, nodeId)
}

func (le *leader_election) runElectionTimeout(roundID uint64, nodeId int) {
	timer := time.NewTimer(le.ackTimeout)
	defer timer.Stop()
	gotAck := false

	for {
		select {
		case r := <-le.ackCh:
			if r != roundID {
				// outdated ack for previous round; ignore
				continue
			}

			gotAck = true

			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			timer.Reset(le.coordTimeout)

		case cr := <-le.coordCh:
			if cr != roundID {
				// outdated coordinator for previous round
				continue
			}
			logger.Logger.Debugf("Node %d received COORDINATOR for round %d", le.id, roundID)
			return

		case <-timer.C:
			if gotAck {
				logger.Logger.Infof("Coordinator timeout expired on node %d; restarting election", le.id)
				go le.handleElectionMsg(nodeId)
				return
			} else {
				logger.Logger.Infof("ACK timeout expired on node %d; no higher nodes responded, becoming leader", le.id)
				le.sendCoordinatorMessage()
				le.leaderId = le.id
				logger.Logger.Infof("Node %d became coordinator", le.id)
				return
			}
		}
	}
}

func (le *leader_election) sendAckMessage(nodeId int) {
	senderMiddleware, _ := le.connectedNodes[nodeId]

	ackMsg := &protocol.SyncMessage{
		NodeId: int32(le.id),
		Action: int32(enum.ACK),
	}

	ackBytes, err := proto.Marshal(ackMsg)
	if err != nil {
		logger.Logger.Errorf("Failed to marshal ACK message: %v", err)
		return
	}

	if e := senderMiddleware.Send(ackBytes); e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("Failed to send ACK to node %d: %d", nodeId, int(e))
	}
}

func (le *leader_election) sendCoordinatorMessage() {
	coordMsg := &protocol.SyncMessage{
		NodeId: int32(le.id),
		Action: int32(enum.COORDINATOR),
	}
	coordBytes, errMarshal := proto.Marshal(coordMsg)
	if errMarshal != nil {
		logger.Logger.Errorf("Failed to marshal COORDINATOR message: %v", errMarshal)
		return
	}

	if e := le.coordMiddleware.Send(coordBytes); e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("Failed to send COORDINATOR message")
	}
}
