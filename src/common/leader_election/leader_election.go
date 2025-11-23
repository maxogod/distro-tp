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
const (
	TIMEOUT_INTERVAL   = 5
	HEARTBEAT_INTERVAL = 1
	ELECTION_TIMEOUT   = 10

	DEFAULT_PORT_PREFIX = 909

	MAX_CHAN_BUFFER = 2000
)

type leaderElection struct {
	running          atomic.Bool
	readyForElection atomic.Bool

	id              int32
	leaderId        atomic.Int32
	middlewareUrl   string
	workerType      enum.WorkerType
	maxNodes        int
	updateCallbacks *UpdateCallbacks

	// Middlewares
	coordMiddleware middleware.MessageMiddleware
	connMiddleware  middleware.MessageMiddleware
	nodeMiddleware  middleware.MessageMiddleware
	connectedNodes  map[int32]middleware.MessageMiddleware

	messagesCh        chan *protocol.SyncMessage
	updatesCh         chan *protocol.DataEnvelope
	isSendingUpdates  atomic.Bool
	routineShutdownCh chan bool

	round               uint64
	ackCh               chan uint64
	coordCh             chan uint64
	ackTimeout          time.Duration
	coordTimeout        time.Duration
	leaderFinderTimeout time.Duration

	// Heartbeat handler
	heartbeatHandler heartbeat.HeartBeatHandler
}

// NewLeaderElection instantiates a new `Bully algorithm` leader election object
// and connects to necessary middlewares.
func NewLeaderElection(
	id int32,
	middlewareUrl string,
	workerType enum.WorkerType,
	maxNodes int,
	updateCallbacks *UpdateCallbacks,
) LeaderElection {
	le := &leaderElection{
		id:              id,
		middlewareUrl:   middlewareUrl,
		workerType:      workerType,
		maxNodes:        maxNodes,
		updateCallbacks: updateCallbacks,

		coordMiddleware: middleware.GetLeaderElectionCoordExchange(middlewareUrl, workerType),
		connMiddleware:  middleware.GetLeaderElectionDiscoveryExchange(middlewareUrl, workerType),
		nodeMiddleware:  middleware.GetLeaderElectionReceivingNodeExchange(middlewareUrl, workerType, strconv.Itoa(int(id))),
		connectedNodes:  make(map[int32]middleware.MessageMiddleware),

		messagesCh:        make(chan *protocol.SyncMessage, MAX_CHAN_BUFFER),
		updatesCh:         make(chan *protocol.DataEnvelope, MAX_CHAN_BUFFER),
		routineShutdownCh: make(chan bool),

		ackCh:   make(chan uint64, 16),
		coordCh: make(chan uint64, 16),

		//TODO: This has to be backed up with facts
		ackTimeout:          2 * time.Second,
		coordTimeout:        5 * time.Second,
		leaderFinderTimeout: 10 * time.Second,
	}

	host := fmt.Sprintf("%s%d", workerType, id)
	port := DEFAULT_PORT_PREFIX + int(id)
	le.heartbeatHandler = heartbeat.NewListeningHeartBeatHandler(host, port, HEARTBEAT_INTERVAL)

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

func (le *leaderElection) Start() error {
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
		case int32(enum.UPDATE):
			if le.IsLeader() {
				go le.startSendingUpdates(nodeID)
			} else {
				le.handleUpdateMsg(msg.GetEnvelope())
			}
		default:
			logger.Logger.Warnf("Unknown leader election action received: %d", msg.GetAction())
		}
	}

	return nil
}

func (le *leaderElection) Close() error {
	le.running.Store(false)
	close(le.updatesCh)
	le.routineShutdownCh <- true // Close node listener
	le.routineShutdownCh <- true // Close leader search timer
	if le.isSendingUpdates.Load() {
		le.routineShutdownCh <- true // Close sending routine
	}
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
		case <-le.routineShutdownCh:
			return
		case <-timer.C:
			logger.Logger.Debug("Leader Not Found - Timeout!")
			onTimeoutFunc()
		}
	}()

	return leaderFoundCh
}

/* --- LISTENERS --- */

// nodeQueueListener should run in a go routine and will get the messages from the middleware
// and forward them into the messages channel.
func (le *leaderElection) nodeQueueListener(readyCh chan bool) {
	e := le.nodeMiddleware.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		readyCh <- true
		running := true
		for running {
			var msg middleware.MessageDelivery
			select {
			case m := <-consumeChannel:
				msg = m
			case <-le.routineShutdownCh:
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

	addrs := []string{}
	for i := range le.maxNodes {
		addr := fmt.Sprintf("%s%d:%d", string(le.workerType), i, DEFAULT_PORT_PREFIX+i)
		addrs = append(addrs, addr)
	}

	err := le.heartbeatHandler.StartSendingToAll(addrs)
	if err != nil {
		logger.Logger.Errorf("Error starting to send heartbeats: %v", err)
	}
}
