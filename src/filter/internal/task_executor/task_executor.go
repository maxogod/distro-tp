package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/filter/business"
	"google.golang.org/protobuf/proto"
)

type FilterExecutor struct {
	config           TaskConfig
	url              string
	filterService    business.FilterService
	connectedClients map[string]middleware.MessageMiddleware
	processedQueues  map[string]middleware.MessageMiddleware
	groupByQueue     middleware.MessageMiddleware
}

func NewFilterExecutor(config TaskConfig,
	url string,
	filterService business.FilterService,
	connectedClients map[string]middleware.MessageMiddleware,
	groupByQueue middleware.MessageMiddleware,
	processedQueue map[string]middleware.MessageMiddleware) worker.TaskExecutor {
	return &FilterExecutor{
		config:           config,
		url:              url,
		filterService:    filterService,
		connectedClients: connectedClients,
		processedQueues:  processedQueue,
		groupByQueue:     groupByQueue,
	}
}

func (fe *FilterExecutor) HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	transactionBatch := &raw.TransactionBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()
	_, exists := fe.connectedClients[clientID]
	if !exists {
		fe.connectedClients[clientID] = middleware.GetCounterExchange(fe.url, clientID+"@"+string(enum.FilterWorker))
	}
	counterExchange := fe.connectedClients[clientID]

	_, exists = fe.processedQueues[clientID]
	if !exists {
		fe.processedQueues[clientID] = middleware.GetProcessedDataExchange(fe.url, clientID)
	}
	m := fe.processedQueues[clientID]

	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	fe.filterService.FilterByYear(transactionBatch)
	fe.filterService.FilterByTime(transactionBatch)
	fe.filterService.FilterByFinalAmount(transactionBatch)

	amountSent := 0
	if len(transactionBatch.Transactions) != 0 {
		err = worker.SendDataToMiddleware(transactionBatch, enum.T1, clientID, int(dataEnvelope.GetSequenceNumber()), m)
		if err != nil {
			shouldRequeue = true
			return err
		}
		amountSent = 1
	}
	shouldAck = true

	if err := worker.SendCounterMessage(clientID, amountSent, int(dataEnvelope.SequenceNumber), enum.FilterWorker, enum.None, counterExchange); err != nil {
		return err
	}

	return nil
}

func (fe *FilterExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	transactionBatch := &raw.TransactionItemsBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()
	_, exists := fe.connectedClients[clientID]
	if !exists {
		fe.connectedClients[clientID] = middleware.GetCounterExchange(fe.url, clientID+"@"+string(enum.FilterWorker))
	}
	counterExchange := fe.connectedClients[clientID]

	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	fe.filterService.FilterItemsByYear(transactionBatch)

	amountSent := 0
	if len(transactionBatch.TransactionItems) != 0 {
		err = worker.SendDataToMiddleware(transactionBatch, enum.T2, clientID, int(dataEnvelope.GetSequenceNumber()), fe.groupByQueue)
		if err != nil {
			shouldRequeue = true
			return err
		}
		amountSent = 1
	}
	shouldAck = true

	if err := worker.SendCounterMessage(clientID, amountSent, int(dataEnvelope.SequenceNumber), enum.FilterWorker, enum.AggregatorWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (fe *FilterExecutor) HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	transactionBatch := &raw.TransactionBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()
	_, exists := fe.connectedClients[clientID]
	if !exists {
		fe.connectedClients[clientID] = middleware.GetCounterExchange(fe.url, clientID+"@"+string(enum.FilterWorker))
	}
	counterExchange := fe.connectedClients[clientID]

	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	fe.filterService.FilterByYear(transactionBatch)
	fe.filterService.FilterByTime(transactionBatch)

	amountSent := 0
	if len(transactionBatch.Transactions) != 0 {
		err = worker.SendDataToMiddleware(transactionBatch, enum.T3, clientID, int(dataEnvelope.GetSequenceNumber()), fe.groupByQueue)
		if err != nil {
			shouldRequeue = true
			return err
		}
		amountSent = 1
	}
	shouldAck = true

	if err := worker.SendCounterMessage(clientID, amountSent, int(dataEnvelope.SequenceNumber), enum.FilterWorker, enum.AggregatorWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (fe *FilterExecutor) HandleTask4(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	transactionBatch := &raw.TransactionBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()
	_, exists := fe.connectedClients[clientID]
	if !exists {
		fe.connectedClients[clientID] = middleware.GetCounterExchange(fe.url, clientID+"@"+string(enum.FilterWorker))
	}
	counterExchange := fe.connectedClients[clientID]

	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	fe.filterService.FilterByYear(transactionBatch)
	fe.filterService.FilterNullUserIDs(transactionBatch)

	amountSent := 0
	if len(transactionBatch.Transactions) != 0 {
		err = worker.SendDataToMiddleware(transactionBatch, enum.T4, clientID, int(dataEnvelope.GetSequenceNumber()), fe.groupByQueue)
		if err != nil {
			shouldRequeue = true
			return err
		}
		amountSent = 1
	}
	shouldAck = true

	if err := worker.SendCounterMessage(clientID, amountSent, int(dataEnvelope.SequenceNumber), enum.FilterWorker, enum.AggregatorWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (fe *FilterExecutor) Close() error {
	if e := fe.groupByQueue.Close(); e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close group by queue: %v", e)
	}

	for clientID, counterExchange := range fe.connectedClients {
		if e := counterExchange.Close(); e != middleware.MessageMiddlewareSuccess {
			return fmt.Errorf("failed to close counter exchange for client %s: %v", clientID, e)
		}
	}

	for clientID, processedExchange := range fe.processedQueues {
		if e := processedExchange.Close(); e != middleware.MessageMiddlewareSuccess {
			return fmt.Errorf("failed to close processed exchange for client %s: %v", clientID, e)
		}
	}

	return nil
}

func (fe *FilterExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	defer ackHandler(true, false)

	clientID := dataEnvelope.GetClientId()

	_, exists := fe.processedQueues[clientID]
	if !exists {
		fe.processedQueues[clientID] = middleware.GetProcessedDataExchange(fe.url, clientID)
	}
	m := fe.processedQueues[clientID]

	if dataEnvelope.GetTaskType() == int32(enum.T1) { // No more layers for this task
		worker.SendDone(dataEnvelope.GetClientId(), enum.T1, m)
	}

	delete(fe.processedQueues, clientID)

	return nil
}
