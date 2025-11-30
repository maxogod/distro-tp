package task_executor

import (
	"fmt"
	"sync"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/leader_election"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/worker"
)

const DELETE_ACTION = -1

type AggregatorExecutor struct {
	config            *config.Config // TODO: maybe remove config from here
	connectedClients  map[string]middleware.MessageMiddleware
	aggregatorService business.AggregatorService
	finishExecutor    FinishExecutor

	leaderElection  leader_election.LeaderElection
	finishedClients *sync.Map // map[string]enum.TaskType
}

func NewAggregatorExecutor(config *config.Config,
	connectedClients map[string]middleware.MessageMiddleware,
	aggregatorService business.AggregatorService,
	finishExecutor FinishExecutor,
	leaderElection leader_election.LeaderElection,
	finishedClients *sync.Map,
) worker.TaskExecutor {
	return &AggregatorExecutor{
		config:            config,
		connectedClients:  connectedClients,
		aggregatorService: aggregatorService,
		finishExecutor:    finishExecutor,
		leaderElection:    leaderElection,
		finishedClients:   finishedClients,
	}
}

func (ae *AggregatorExecutor) HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	ackHandler(false, false)
	logger.Logger.Warn("Aggregator should not handle task 1!")
	return nil
}

func (ae *AggregatorExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	defer ackHandler(shouldAck, false)
	clientID := dataEnvelope.GetClientId()

	ae.aggregatorService.StoreData(clientID, dataEnvelope)
	shouldAck = true

	_, exists := ae.connectedClients[clientID]
	if !exists {
		ae.connectedClients[clientID] = middleware.GetCounterExchange(ae.config.Address, clientID+"@"+string(enum.AggregatorWorker))
	}
	counterExchange := ae.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 0, int(dataEnvelope.SequenceNumber), enum.AggregatorWorker, enum.None, counterExchange); err != nil {
		return err
	}

	return nil
}

func (ae *AggregatorExecutor) HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	defer ackHandler(shouldAck, false)
	clientID := dataEnvelope.GetClientId()

	ae.aggregatorService.StoreData(clientID, dataEnvelope)
	shouldAck = true

	_, exists := ae.connectedClients[clientID]
	if !exists {
		ae.connectedClients[clientID] = middleware.GetCounterExchange(ae.config.Address, clientID+"@"+string(enum.AggregatorWorker))
	}
	counterExchange := ae.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 0, int(dataEnvelope.SequenceNumber), enum.AggregatorWorker, enum.None, counterExchange); err != nil {
		return err
	}

	return nil
}

func (ae *AggregatorExecutor) HandleTask4(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	defer ackHandler(shouldAck, false)

	clientID := dataEnvelope.GetClientId()
	ae.aggregatorService.StoreData(clientID, dataEnvelope)
	shouldAck = true

	_, exists := ae.connectedClients[clientID]
	if !exists {
		ae.connectedClients[clientID] = middleware.GetCounterExchange(ae.config.Address, clientID+"@"+string(enum.AggregatorWorker))
	}
	counterExchange := ae.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 0, int(dataEnvelope.SequenceNumber), enum.AggregatorWorker, enum.None, counterExchange); err != nil {
		return err
	}
	return nil
}

func (ae *AggregatorExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	defer ackHandler(shouldAck, false)

	clientID := dataEnvelope.GetClientId()
	taskType := dataEnvelope.GetTaskType()

	ae.finishedClients.LoadOrStore(clientID, taskType)

	if dataEnvelope.GetSequenceNumber() == DELETE_ACTION {
		logger.Logger.Debugf("Deleting client data for: %s", clientID)
		ae.removeClientData(clientID)
		shouldAck = true
		return nil
	}

	// only the leader should finish clients
	if !ae.leaderElection.IsLeader() {
		shouldAck = true
		return nil
	}

	if taskType == int32(enum.T1) {
		shouldAck = true
		return nil
	}

	logger.Logger.Debugf("Finishing client: %s | task-type: %d", clientID, taskType)

	// we finish clients asynchronously to not block the ack of the finish message
	go func() {
		err := ae.finishExecutor.SendAllData(clientID, enum.TaskType(taskType))
		if err != nil {
			logger.Logger.Errorf("Error finishing client %s for task %d: %v", clientID, taskType, err)
		}
		logger.Logger.Debug("Client Finished: ", clientID)
		shouldAck = true
	}()

	return nil
}

func (ae *AggregatorExecutor) Close() error {
	if err := ae.aggregatorService.Close(); err != nil {
		return fmt.Errorf("failed to close aggregator service: %v", err)
	}

	for clientID, q := range ae.connectedClients {
		if e := q.Close(); e != middleware.MessageMiddlewareSuccess {
			logger.Logger.Errorf("failed to close middleware for client %s: %v", clientID, e)
		}
	}

	return nil
}

func (ae *AggregatorExecutor) removeClientData(clientID string) {
	//ae.aggregatorService.RemoveData(clientID)
	if q, exists := ae.connectedClients[clientID]; exists {
		q.Close()
		delete(ae.connectedClients, clientID)
	}
	ae.finishedClients.LoadAndDelete(clientID)
	logger.Logger.Debugf("Removed client data for: %s", clientID)
}



