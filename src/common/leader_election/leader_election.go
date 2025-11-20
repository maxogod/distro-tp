package leader_election

import (
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/utils"
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
		nodeMiddleware:  middleware.GetLeaderElectionNodeExchange(middlewareUrl, workerType, strconv.Itoa(id)),

		updateChan:     make(chan *protocol.DataEnvelope),
		connectedNodes: make(map[int]middleware.MessageMiddleware),

		messagesCh: make(chan *protocol.SyncMessage),
		shutdownCh: make(chan bool),
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
				le.connectedNodes[nodeID] = middleware.GetLeaderElectionNodeExchange(le.url, le.workerType, strconv.Itoa(nodeID))
			}
		case int32(enum.COORDINATOR):
			le.leaderId = nodeID
			logger.Logger.Infof("Node %d recognized as coordinator", nodeID)
		case int32(enum.ELECTION):
			// TODO: implement election logic
		case int32(enum.ACK):
			// TODO: implement ack logic
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
