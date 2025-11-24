package leader_election

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/leader_election/handlers"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

// TODO: THIS MUST BE BACKED UP WITH FACTS!!!
const (
	ACK_TIMEOUT         = 2 * time.Second
	COORDINATOR_TIMEOUT = 6 * time.Second
	HEARTBEAT_INTERVAL  = 100 * time.Millisecond

	DEFAULT_HOST = "localhost"
	DEFAULT_PORT = 9090

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

	messagesCh       chan *protocol.SyncMessage
	updatesCh        chan *protocol.DataEnvelope
	isSendingUpdates atomic.Bool
	ctx              context.Context
	cancel           context.CancelFunc

	// Handlers
	heartbeatHandler heartbeat.HeartBeatHandler
	electionHandler  handlers.ElectionHandler
}

// NewLeaderElection instantiates a new `Bully algorithm` leader election object
// and connects to necessary middlewares.
func NewLeaderElection(
	hostName string,
	heartbeatPort int,
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

		messagesCh: make(chan *protocol.SyncMessage, MAX_CHAN_BUFFER),
		updatesCh:  make(chan *protocol.DataEnvelope, MAX_CHAN_BUFFER),
	}
	le.ctx, le.cancel = context.WithCancel(context.Background())

	for i := 1; i <= maxNodes; i++ {
		if i == int(id) {
			continue
		}
		logger.Logger.Debugf("[Node %d] connecting to node %d middleware", le.id, i)
		le.connectedNodes[int32(i)] = middleware.GetLeaderElectionSendingNodeExchange(middlewareUrl, workerType, strconv.Itoa(i))
	}

	heartbeatHandler, err := heartbeat.NewListeningHeartBeatHandler(hostName, heartbeatPort, HEARTBEAT_INTERVAL)
	if err != nil {
		logger.Logger.Errorf("[Node %d] Error creating heartbeat handler, cannot start election object: %v", le.id, err)
		return nil
	}
	le.heartbeatHandler = heartbeatHandler

	le.electionHandler = handlers.NewElectionHandler(id, le.connectedNodes, le.coordMiddleware, ACK_TIMEOUT, COORDINATOR_TIMEOUT)

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

	leaderSearchTimerCh := le.startDiscoveryPhase()

	for le.running.Load() {
		msg := <-le.messagesCh
		nodeID := msg.GetNodeId()
		switch msg.GetAction() {
		case int32(enum.DISCOVER):
			le.handleDiscoverMsg(nodeID, msg.GetLeaderId(), &leaderSearchTimerCh)
		case int32(enum.COORDINATOR):
			le.handleCoordinatorMsg(nodeID)
		case int32(enum.ELECTION):
			if le.readyForElection.Load() { // The node is ready for election after loading all of the data
				le.electionHandler.HandleElectionMessage(nodeID, msg.GetRoundId())
			}
		case int32(enum.ACK):
			logger.Logger.Infof("[Node %d] GOT ACK FROM: %d", le.id, nodeID)
			le.electionHandler.HandleAckMessage(msg.GetRoundId())
		case int32(enum.UPDATE):
			if le.IsLeader() {
				go le.startSendingUpdates(nodeID)
			} else {
				le.handleUpdateMsg(msg.GetEnvelope())
			}
		default:
			logger.Logger.Warnf("[Node %d] Unknown leader election action received: %d", le.id, msg.GetAction())
		}
	}

	return nil
}

func (le *leaderElection) Close() error {
	le.running.Store(false)
	close(le.updatesCh)
	le.cancel()
	if le.isSendingUpdates.Load() {
		// TODO: check if this boolean is needed
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
		timer := time.NewTimer(COORDINATOR_TIMEOUT)
		defer close(leaderFoundCh)
		defer timer.Stop()

		select {
		case <-leaderFoundCh:
			logger.Logger.Debugf("[Node %d] Leader Found before Timeout!", le.id)
			return
		case <-le.ctx.Done():
			return
		case timeout := <-timer.C:
			elapsed := fmt.Sprintf("%.2f", time.Since(timeout.Add(-COORDINATOR_TIMEOUT)).Seconds())
			logger.Logger.Debugf("[Node %d] Leader Not Found after %s seconds - Timeout!", le.id, elapsed)
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
			case <-le.ctx.Done():
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
		le.startReceivingHeartbeats()
	}
}

func (le *leaderElection) startReceivingHeartbeats() {
	initElectionFunc := func(timeoutAmount int) {
		logger.Logger.Infof("[Node %d] Leader Heartbeat Timeout Detected! Starting Election...", le.id)
		le.heartbeatHandler.Stop() // Stop receiving heartbeats
		le.electionHandler.StartElection()
	}

	err := le.heartbeatHandler.StartReceiving(initElectionFunc, ACK_TIMEOUT)
	if err != nil {
		logger.Logger.Errorf("Error starting to receive heartbeats: %v", err)
	}

}

func (le *leaderElection) startSendingHeartbeats() {
	addrs := []string{}
	for i := 1; i <= le.maxNodes; i++ {
		if i == int(le.id) {
			continue
		}

		addr := fmt.Sprintf("%s%d:%d", string(le.workerType), i, DEFAULT_PORT)
		if le.workerType == enum.None { // Use default localhost for TESTING
			addr = DEFAULT_HOST + ":" + strconv.Itoa(DEFAULT_PORT+i)
		}
		addrs = append(addrs, addr)
	}

	err := le.heartbeatHandler.StartSendingToAll(addrs)
	if err != nil {
		logger.Logger.Errorf("Error starting to send heartbeats: %v", err)
	}
}

func (le *leaderElection) handleCoordinatorMsg(nodeId int32) {
	if le.id > nodeId && le.electionHandler.IsElectionRunning() {
		logger.Logger.Debugf("[Node %d] Received COORDINATOR from Node %d, but my ID is higher. Ignoring...", le.id, nodeId)
		return
	}
	le.leaderId.Store(nodeId)
	le.electionHandler.StopElection()
	le.beginHeartbeatHandler()
	logger.Logger.Infof("[Node %d] recognized Node %d as leader", le.id, nodeId)
}
