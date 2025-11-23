package leader_election

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

// TODO: THIS MUST BE BACKED UP WITH FACTS!!!
const TIMEOUT_INTERVAL = 5
const HEARTBEAT_INTERVAL = 1
const ELECTION_TIMEOUT = 10

const DEAFULT_PORT = 9090

type leaderElection struct {
	running          atomic.Bool
	readyForElection atomic.Bool

	id            int32
	leaderId      atomic.Int32
	middlewareUrl string
	workerType    enum.WorkerType
	maxNodes      int

	// Middlewares
	coordMiddleware middleware.MessageMiddleware
	connMiddleware  middleware.MessageMiddleware
	nodeMiddleware  middleware.MessageMiddleware
	connectedNodes  map[int32]middleware.MessageMiddleware

	messagesCh         chan *protocol.SyncMessage
	listenerShutdownCh chan bool

	round               uint64
	ackCh               chan uint64
	coordCh             chan uint64
	ackTimeout          time.Duration
	coordTimeout        time.Duration
	leaderFinderTimeout time.Duration

	// Heartbeat handler
	heartbeatHandler heartbeat.HeartBeatHandler
}

func NewLeaderElection(
	id int32,
	middlewareUrl string,
	workerType enum.WorkerType,
	maxNodes int,
) LeaderElection {
	le := &leaderElection{
		id:            id,
		middlewareUrl: middlewareUrl,
		workerType:    workerType,
		maxNodes:      maxNodes,

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

		heartbeatHandler: heartbeat.NewReverseHeartBeatHandler(TIMEOUT_INTERVAL),
	}

	for i := range int32(maxNodes) {
		if i == id {
			continue
		}
		le.connectedNodes[i] = middleware.GetLeaderElectionSendingNodeExchange(middlewareUrl, workerType, strconv.Itoa(int(i)))
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

	// This should only timeout at startup
	leaderSearchTimerCh := le.initLeaderSearchTimer(func() {
		// There is no leader -> start election
		le.readyForElection.Store(true)
		le.startElection()
	})

	le.readyForElection.Store(true)

	for le.running.Load() {
		msg := <-le.messagesCh
		nodeID := msg.GetNodeId()
		switch msg.GetAction() {
		case int32(enum.DISCOVER):
			le.handleDiscoverMsg(nodeID, msg.GetLeaderId(), leaderSearchTimerCh)
		case int32(enum.COORDINATOR):
			le.handleCoordinatorMsg(nodeID)
			// select {
			// case le.coordCh <- atomic.LoadUint64(&le.round):
			// default:
			// }
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
	le.listenerShutdownCh <- true // Close node listener
	le.listenerShutdownCh <- true // Close leader search timer
	le.heartbeatHandler.Close()

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

func (le *leaderElection) initLeaderSearchTimer(onTimeoutFunc func()) chan bool {
	leaderFoundCh := make(chan bool)

	go func() {
		defer close(leaderFoundCh)
		timer := time.NewTimer(le.leaderFinderTimeout)
		defer timer.Stop()

		select {
		case <-leaderFoundCh:
			return
		case <-le.listenerShutdownCh:
			return
		case <-timer.C:
			logger.Logger.Debug("Leader Not Found - Timeout!")
			onTimeoutFunc()
		}
	}()

	return leaderFoundCh
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

/* --- Heartbeat Handler --- */

func (le *leaderElection) beginHeartbeatHandler() {
	le.heartbeatHandler.Stop() // Stop any ongoing heartbeat process
	if le.IsLeader() {
		le.startSendingHeartbeats()
	} else {
		le.startRecievingHeartbeats()
	}
}

func (le *leaderElection) startRecievingHeartbeats() {
	// non-leader nodes receive heartbeats from the leader
	if le.IsLeader() {
		return
	}
	host := fmt.Sprintf("%s%d", le.workerType, le.leaderId.Load())
	port := DEAFULT_PORT + int(le.leaderId.Load())
	le.heartbeatHandler.ChangeAddress(host, port)

	initElectionFunc := func(timeoutAmount int) {
		logger.Logger.Infof("Node %d: Leader Heartbeat Timeout Detected! Starting Election...", le.id)
		le.heartbeatHandler.Stop() // Stop receiving heartbeats
		le.startElection()
	}

	err := le.heartbeatHandler.StartReceiving(initElectionFunc, TIMEOUT_INTERVAL)
	if err != nil {
		logger.Logger.Errorf("Error starting to receive heartbeats: %v", err)
	}

}

func (le *leaderElection) startSendingHeartbeats() {
	// Only the leader sends heartbeats
	if !le.IsLeader() {
		return
	}

	addrs := []string{}
	for i := range le.maxNodes {
		addr := fmt.Sprintf("%s%d:%d", string(le.workerType), i, DEAFULT_PORT+i)
		addrs = append(addrs, addr)
	}

	err := le.heartbeatHandler.StartSendingToAll(addrs, ELECTION_TIMEOUT)
	if err != nil {
		logger.Logger.Errorf("Error starting to send heartbeats: %v", err)
	}
}
