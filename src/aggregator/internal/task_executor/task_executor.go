package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
	"google.golang.org/protobuf/proto"
)

type AggregatorExecutor struct {
	config            *config.Config // TODO: maybe remove config from here
	connectedClients  map[string]middleware.MessageMiddleware
	aggregatorService business.AggregatorService
	finishExecutor    FinishExecutor
}

func NewAggregatorExecutor(config *config.Config,
	connectedClients map[string]middleware.MessageMiddleware,
	aggregatorService business.AggregatorService,
	outputQueue middleware.MessageMiddleware,
) worker.TaskExecutor {
	return &AggregatorExecutor{
		config:            config,
		connectedClients:  connectedClients,
		aggregatorService: aggregatorService,
		finishExecutor:    NewFinishExecutor(config.Address, aggregatorService, outputQueue, config.Limits),
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

	reducedData := &reduced.TotalSumItemsBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	for _, profit := range reducedData.GetTotalSumItems() {
		err = ae.aggregatorService.StoreTotalItems(clientID, profit)
		if err != nil {
			return err
		}
	}
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

	reducedData := &reduced.TotalPaymentValueBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	for _, tpv := range reducedData.GetTotalPaymentValues() {
		err = ae.aggregatorService.StoreTotalPaymentValue(clientID, tpv)
		if err != nil {
			return err
		}
	}
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

	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	countedDataBatch := &reduced.CountedUserTransactionBatch{}
	err := proto.Unmarshal(payload, countedDataBatch)
	if err != nil {
		return err
	}

	for _, countedData := range countedDataBatch.GetCountedUserTransactions() {
		err = ae.aggregatorService.StoreCountedUserTransactions(clientID, countedData)
		if err != nil {
			return err
		}
	}
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

	if taskType == int32(enum.T1) {
		shouldAck = true
		return nil
	}

	logger.Logger.Debugf("Finishing client: %s | task-type: %d", clientID, taskType)

	err := ae.finishExecutor.SendAllData(clientID, enum.TaskType(taskType))
	if err != nil {
		return err
	}

	logger.Logger.Debug("Client Finished: ", clientID)
	shouldAck = true
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
