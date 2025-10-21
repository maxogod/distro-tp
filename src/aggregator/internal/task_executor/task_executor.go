package task_executor

import (
	"fmt"
	"strings"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

// To differentiate between Task 2.1 and Task 2.2 results in the DB
const T2_1_PREFIX = "T2_1@"
const T2_2_PREFIX = "T2_2@"

type AggregatorExecutor struct {
	config            *config.Config
	connectedClients  map[string]middleware.MessageMiddleware
	aggregatorService business.AggregatorService
	finishExecutor    FinishExecutor
	clientTasks       map[string]enum.TaskType
}

func NewAggregatorExecutor(config *config.Config,
	connectedClients map[string]middleware.MessageMiddleware,
	aggregatorService business.AggregatorService) worker.TaskExecutor {
	return &AggregatorExecutor{
		config:            config,
		connectedClients:  connectedClients,
		aggregatorService: aggregatorService,
		finishExecutor:    NewFinishExecutor(config.Address, aggregatorService),
		clientTasks:       make(map[string]enum.TaskType),
	}
}

func (ae *AggregatorExecutor) HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	defer ackHandler(shouldAck, false)

	transactionBatch := &raw.TransactionBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	ae.clientTasks[clientID] = enum.T1

	err = ae.aggregatorService.StoreTransactions(clientID, transactionBatch.Transactions)
	if err != nil {
		return err
	}
	shouldAck = true

	_, exists := ae.connectedClients[clientID]
	if !exists {
		ae.connectedClients[clientID] = middleware.GetCounterExchange(ae.config.Address, clientID+"@"+string(enum.AggregatorWorker))
	}
	counterExchange := ae.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 0, enum.AggregatorWorker, enum.None, counterExchange); err != nil {
		return err
	}

	return nil
}

func (ae *AggregatorExecutor) HandleTask2_1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	defer ackHandler(shouldAck, false)

	reducedData := &reduced.TotalProfitBySubtotal{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	ae.clientTasks[clientID] = enum.T2

	// To differentiate between Task 2.1 and Task 2.2 results in the DB
	clientID = T2_1_PREFIX + clientID

	err = ae.aggregatorService.StoreTotalProfitBySubtotal(clientID, reducedData)
	if err != nil {
		return err
	}
	shouldAck = true

	realClientID := strings.Split(clientID, "@")[1]
	_, exists := ae.connectedClients[clientID]
	if !exists {
		ae.connectedClients[clientID] = middleware.GetCounterExchange(ae.config.Address, realClientID+"@"+string(enum.AggregatorWorker))
	}
	counterExchange := ae.connectedClients[clientID]
	if err := worker.SendCounterMessage(realClientID, 0, enum.AggregatorWorker, enum.None, counterExchange); err != nil {
		return err
	}

	return nil
}

func (ae *AggregatorExecutor) HandleTask2_2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	defer ackHandler(shouldAck, false)

	reducedData := &reduced.TotalSoldByQuantity{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	ae.clientTasks[clientID] = enum.T2

	// To differentiate between Task 2.1 and Task 2.2 results in the DB
	clientID = T2_2_PREFIX + clientID

	err = ae.aggregatorService.StoreTotalSoldByQuantity(clientID, reducedData)
	if err != nil {
		return err
	}
	shouldAck = true

	realClientID := strings.Split(clientID, "@")[1]
	_, exists := ae.connectedClients[clientID]
	if !exists {
		ae.connectedClients[clientID] = middleware.GetCounterExchange(ae.config.Address, realClientID+"@"+string(enum.AggregatorWorker))
	}
	counterExchange := ae.connectedClients[clientID]
	if err := worker.SendCounterMessage(realClientID, 0, enum.AggregatorWorker, enum.None, counterExchange); err != nil {
		return err
	}

	return nil
}

func (ae *AggregatorExecutor) HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	defer ackHandler(shouldAck, false)

	reducedData := &reduced.TotalPaymentValue{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	ae.clientTasks[clientID] = enum.T3

	err = ae.aggregatorService.StoreTotalPaymentValue(clientID, reducedData)
	if err != nil {
		return err
	}
	shouldAck = true

	_, exists := ae.connectedClients[clientID]
	if !exists {
		ae.connectedClients[clientID] = middleware.GetCounterExchange(ae.config.Address, clientID+"@"+string(enum.AggregatorWorker))
	}
	counterExchange := ae.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 0, enum.AggregatorWorker, enum.None, counterExchange); err != nil {
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
		ae.clientTasks[clientID] = enum.T4
		err = ae.aggregatorService.StoreCountedUserTransactions(clientID, countedData)
		if err != nil {
			return err
		}
		shouldAck = true
	}

	_, exists := ae.connectedClients[clientID]
	if !exists {
		ae.connectedClients[clientID] = middleware.GetCounterExchange(ae.config.Address, clientID+"@"+string(enum.AggregatorWorker))
	}
	counterExchange := ae.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 0, enum.AggregatorWorker, enum.None, counterExchange); err != nil {
		return err
	}
	return nil
}

func (ae *AggregatorExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	// TODO: IMPORTANT: HAVE  THE SORT AND SEND DATA BE IN A SEPERATE GO ROUTINE!
	shouldAck := false
	defer ackHandler(shouldAck, false)

	clientID := dataEnvelope.GetClientId()
	taskType, exists := ae.clientTasks[clientID]
	log.Debugf("Finishing client: %s | task-type: %d", clientID, taskType)
	if !exists {
		log.Warn("Client ID never sent any data: ", clientID)
		return nil
	}
	task := enum.TaskType(taskType)
	if task == enum.T2 {
		err := ae.finishExecutor.SendAllData(clientID, enum.T2_1)
		if err != nil {
			log.Debug("Failed to send all data for client: ", clientID, " | error: ", err)
		}
		err = ae.finishExecutor.SendAllData(clientID, enum.T2_2)
		if err != nil {
			log.Debug("Failed to send all data for client: ", clientID, " | error: ", err)
		}
		log.Debug("Client Finished: ", clientID)
		delete(ae.clientTasks, clientID)
		shouldAck = true
		return nil
	}

	err := ae.finishExecutor.SendAllData(clientID, enum.TaskType(taskType))
	if err != nil {
		return err
	}

	log.Debug("Client Finished: ", clientID)
	delete(ae.clientTasks, clientID)
	shouldAck = true
	return nil
}

func (ae *AggregatorExecutor) Close() error {
	if err := ae.aggregatorService.Close(); err != nil {
		return fmt.Errorf("failed to close aggregator service: %v", err)
	}

	for clientID, q := range ae.connectedClients {
		if e := q.Close(); e != middleware.MessageMiddlewareSuccess {
			log.Errorf("failed to close middleware for client %s: %v", clientID, e)
		}
	}

	return nil
}

func (ae *AggregatorExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The aggregator does not implement Task 2")
}
