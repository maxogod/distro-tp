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

type leaderElection struct {
	running atomic.Bool

	id            int32
	leaderId      atomic.Int32
	middlewareUrl string
	workerType    enum.WorkerType

	// Middlewares
	coordMiddleware middleware.MessageMiddleware
	connMiddleware  middleware.MessageMiddleware
	nodeMiddleware  middleware.MessageMiddleware
	connectedNodes  map[int32]middleware.MessageMiddleware

	messagesCh         chan *protocol.SyncMessage
	listenerShutdownCh chan bool

	readyForElection    atomic.Bool
	round               uint64
	ackCh               chan uint64
	coordCh             chan uint64
	ackTimeout          time.Duration
	coordTimeout        time.Duration
	leaderFinderTimeout time.Duration
}

func NewLeaderElection(
	id int32,
	middlewareUrl string,
	workerType enum.WorkerType,
) LeaderElection {
	le := &leaderElection{
		id:            id,
		middlewareUrl: middlewareUrl,
		workerType:    workerType,

		coordMiddleware: middleware.GetLeaderElectionCoordExchange(middlewareUrl, workerType),
		connMiddleware:  middleware.GetLeaderElectionDiscoveryExchange(middlewareUrl, workerType),
		nodeMiddleware:  middleware.GetLeaderElectionReceivingNodeExchange(middlewareUrl, workerType, strconv.Itoa(int(id))),
		connectedNodes:  make(map[int32]middleware.MessageMiddleware),

		messagesCh:         make(chan *protocol.SyncMessage),
		listenerShutdownCh: make(chan bool),

		ackCh:   make(chan uint64, 16),
		coordCh: make(chan uint64, 16),

		//TODO: This has to be backed up with facts
		ackTimeout:          2 * time.Second,
		coordTimeout:        5 * time.Second,
		leaderFinderTimeout: 10 * time.Second,
	}

	routineReadyCh := make(chan bool)
	go le.nodeQueueListener(routineReadyCh)
	<-routineReadyCh

	return le
}

func (le *leaderElection) IsLeader() bool {
	return le.leaderId.Load() == le.id
}

func (le *leaderElection) FinishClient(clientID string) error {
	if !le.IsLeader() {
		return nil
	}
	// Notify other nodes about client finish
	for _, connMiddleware := range le.connectedNodes {
		msg := protocol.DataEnvelope{ // TODO: ADD FINISH CLIENT ACTION
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

func (le *leaderElection) Start(updateCallbacks *UpdateCallbacks) error {
	le.running.Store(true)
	le.readyForElection.Store(false)

	le.sendDiscoveryMessage()

	leaderTimerCh := le.initLeaderTimer(func() {
		le.readyForElection.Store(true)
		electionMsg := &protocol.SyncMessage{
			NodeId: int32(le.id),
			Action: int32(enum.ELECTION),
		}
		le.messagesCh <- electionMsg
	})

	for le.running.Load() {
		msg := <-le.messagesCh
		nodeID := msg.GetNodeId()
		switch msg.GetAction() {
		case int32(enum.DISCOVER):
			if _, exists := le.connectedNodes[nodeID]; !exists {
				le.connectedNodes[nodeID] = middleware.GetLeaderElectionSendingNodeExchange(le.middlewareUrl, le.workerType, strconv.Itoa(int(nodeID)))
			}
			// When a new node connects, and finds out who is the leader,
			// it updates its leaderId and stops the leader timer
			if msg.GetIsLeader() {
				le.leaderId.Store(nodeID)
				leaderTimerCh <- true // Stop the leader timer
			}
		case int32(enum.COORDINATOR):
			le.leaderId.Store(nodeID)
			logger.Logger.Infof("Node %d recognized as coordinator", nodeID)
			select {
			case le.coordCh <- atomic.LoadUint64(&le.round):
			default:
			}
		case int32(enum.ELECTION):
			logger.Logger.Infof("Node %d received ELECTION from node %d", le.id, nodeID)
			if le.readyForElection.Load() { // The node is ready for election after loading all of the data
				le.handleElectionMsg(nodeID)
			}
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

func (le *leaderElection) Close() error {
	le.running.Store(false)
	le.listenerShutdownCh <- true

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

/* --- PRIVATE METHODS --- */

func (le *leaderElection) initLeaderTimer(onTimeoutFunc func()) chan any {
	timerCh := make(chan any)

	go func() {
		defer close(timerCh)
		timer := time.NewTimer(le.leaderFinderTimeout)
		defer timer.Stop()

		select {
		case <-timerCh:
			logger.Logger.Debug("Leader Found!")
			return
		case <-timer.C:
			// Timeout occurred - run the timeout function
			logger.Logger.Debug("Leader Not Found - Timeout!")
			onTimeoutFunc()
		case <-le.listenerShutdownCh:
			logger.Logger.Warn("Timer cancelled due to shutdown")
			return
		}
	}()

	return timerCh
}

/* --- LISTENERS --- */

func (le *leaderElection) nodeQueueListener(readyCh chan bool) {
	e := le.nodeMiddleware.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		readyCh <- true
		running := true
		for running {
			var msg middleware.MessageDelivery
			select {
			case m := <-consumeChannel:
				msg = m
			case <-le.listenerShutdownCh:
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
