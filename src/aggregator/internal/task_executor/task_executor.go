package task_executor

import (
	"fmt"
	"sync"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/worker"
)

const (
	FLUSH_THRESHOLD = 1000
	DELETE_ACTION   = -1
)

type AggregatorExecutor struct {
	config            *config.Config
	aggregatorService business.AggregatorService
	finishExecutor    FinishExecutor

	ackHandlers *sync.Map // map[clientTask][]func(bool, bool) error
}

func NewAggregatorExecutor(config *config.Config,
	aggregatorService business.AggregatorService,
	outputQueue middleware.MessageMiddleware,
) worker.TaskExecutor {
	ackHandlers := sync.Map{}
	ae := &AggregatorExecutor{
		config:            config,
		aggregatorService: aggregatorService,
		ackHandlers:       &ackHandlers,
		finishExecutor:    NewFinishExecutor(config.Address, aggregatorService, outputQueue, config.Limits, &ackHandlers),
	}
	return ae

}

func (ae *AggregatorExecutor) HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	ackHandler(false, false)
	logger.Logger.Warn("Aggregator should not handle task 1!")
	return nil
}

func (ae *AggregatorExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	clientID := dataEnvelope.GetClientId()
	ae.addToAckHandlers(clientID, ackHandler)
	ae.aggregatorService.StoreData(clientID, dataEnvelope)
	return ae.flushAckHandlers(clientID)
}

func (ae *AggregatorExecutor) HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	clientID := dataEnvelope.GetClientId()
	ae.addToAckHandlers(clientID, ackHandler)
	ae.aggregatorService.StoreData(clientID, dataEnvelope)
	return ae.flushAckHandlers(clientID)

}

func (ae *AggregatorExecutor) HandleTask4(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	clientID := dataEnvelope.GetClientId()
	ae.addToAckHandlers(clientID, ackHandler)
	ae.aggregatorService.StoreData(clientID, dataEnvelope)
	return ae.flushAckHandlers(clientID)
}

func (ae *AggregatorExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	clientID := dataEnvelope.GetClientId()

	if dataEnvelope.GetSequenceNumber() == DELETE_ACTION {
		logger.Logger.Debugf("Deleting client data for: %s", clientID)
		ae.removeClientData(clientID)
		ae.finishExecutor.AckClientMessages(clientID)
		ackHandler(true, false)
		return nil
	}

	taskType := dataEnvelope.GetTaskType()

	if taskType == int32(enum.T1) {
		ackHandler(true, false)
		return nil
	}

	logger.Logger.Debugf("Finishing client: %s | task-type: T%d", clientID, taskType)

	// we finish clients asynchronously to not block the ack of the finish message
	go func() {
		err := ae.finishExecutor.SendAllData(clientID, enum.TaskType(taskType))
		if err != nil {
			logger.Logger.Errorf("Error finishing client %s for task %d: %v", clientID, taskType, err)
		}
		logger.Logger.Debugf("[%s] Client Finished", clientID)
		ackHandler(true, false)
	}()
	return nil
}

func (ae *AggregatorExecutor) Close() error {
	if err := ae.aggregatorService.Close(); err != nil {
		return fmt.Errorf("failed to close aggregator service: %v", err)
	}
	return nil
}

func (ae *AggregatorExecutor) removeClientData(clientID string) {
	ae.aggregatorService.RemoveData(clientID)
	logger.Logger.Debugf("Removed client data for: %s", clientID)
}

func (ae *AggregatorExecutor) addToAckHandlers(clientID string, ackHandler func(bool, bool) error) {
	// Load existing handlers or create new slice
	handlersInterface, _ := ae.ackHandlers.LoadOrStore(clientID, []func(bool, bool) error{})
	handlers := handlersInterface.([]func(bool, bool) error)

	// Append the new handler
	handlers = append(handlers, ackHandler)

	// Store back to map
	ae.ackHandlers.Store(clientID, handlers)
}

func (ae *AggregatorExecutor) flushAckHandlers(clientID string) error {
	value, ok := ae.ackHandlers.Load(clientID)
	if !ok {
		return fmt.Errorf("no ack handlers for client %s", clientID)
	}
	handlers, ok := value.([]func(bool, bool) error)
	if !ok {
		return fmt.Errorf("invalid ack handlers type for client %s", clientID)
	}

	if len(handlers) >= FLUSH_THRESHOLD {
		ae.aggregatorService.FlushData(clientID)
		ae.finishExecutor.AckClientMessages(clientID)
	}
	return nil
}
