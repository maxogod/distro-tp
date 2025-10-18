package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/reducer/business"
	"google.golang.org/protobuf/proto"
)

type GroupByExecutor struct {
	service          business.ReducerService
	url              string
	connectedClients map[string]middleware.MessageMiddleware
	outputQueue      middleware.MessageMiddleware
}

func NewReducerExecutor(filterService business.ReducerService,
	url string,
	connectedClients map[string]middleware.MessageMiddleware,
	reducerQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &GroupByExecutor{
		service:          filterService,
		url:              url,
		connectedClients: connectedClients,
		outputQueue:      reducerQueue,
	}
}

func (re *GroupByExecutor) HandleTask2_1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	groupedItems := &group_by.GroupTransactionItems{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, groupedItems)
	if err != nil {
		return err
	}
	// === Business logic ===
	reducedItems := re.service.SumTotalProfitBySubtotal(groupedItems)

	err = worker.SendDataToMiddleware(reducedItems, enum.T2_1, clientID, re.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	amountSent := 1
	shouldAck = true

	_, exists := re.connectedClients[clientID]
	if !exists {
		re.connectedClients[clientID] = middleware.GetCounterExchange(re.url, clientID)
	}
	counterExchange := re.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, amountSent, enum.ReducerWorker, enum.JoinerWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (re *GroupByExecutor) HandleTask2_2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	groupedItems := &group_by.GroupTransactionItems{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, groupedItems)
	if err != nil {
		return err
	}
	// === Business logic ===
	reducedItems := re.service.SumTotalSoldByQuantity(groupedItems)

	err = worker.SendDataToMiddleware(reducedItems, enum.T2_2, clientID, re.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	amountSent := 1
	shouldAck = true

	_, exists := re.connectedClients[clientID]
	if !exists {
		re.connectedClients[clientID] = middleware.GetCounterExchange(re.url, clientID)
	}
	counterExchange := re.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, amountSent, enum.ReducerWorker, enum.JoinerWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (re *GroupByExecutor) HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	groupTransactions := &group_by.GroupTransactions{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, groupTransactions)
	if err != nil {
		return err
	}
	// === Business logic ===
	reducedTransactions := re.service.SumTotalPaymentValue(groupTransactions)

	err = worker.SendDataToMiddleware(reducedTransactions, enum.T3, clientID, re.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	amountSent := 1
	shouldAck = true

	_, exists := re.connectedClients[clientID]
	if !exists {
		re.connectedClients[clientID] = middleware.GetCounterExchange(re.url, clientID)
	}
	counterExchange := re.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, amountSent, enum.ReducerWorker, enum.JoinerWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (re *GroupByExecutor) HandleTask4(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	groupTransactions := &group_by.GroupTransactions{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, groupTransactions)
	if err != nil {
		return err
	}
	// === Business logic ===
	reducedTransactions := re.service.CountUserTransactions(groupTransactions)

	err = worker.SendDataToMiddleware(reducedTransactions, enum.T4, clientID, re.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	amountSent := 1
	shouldAck = true

	_, exists := re.connectedClients[clientID]
	if !exists {
		re.connectedClients[clientID] = middleware.GetCounterExchange(re.url, clientID)
	}
	counterExchange := re.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, amountSent, enum.ReducerWorker, enum.JoinerWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (re *GroupByExecutor) Close() error {
	if e := re.outputQueue.Close(); e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close reducer queue: %v", e)
	}

	for clientID, q := range re.connectedClients {
		if e := q.Close(); e != middleware.MessageMiddlewareSuccess {
			return fmt.Errorf("failed to close counter exchange for client %s: %v", clientID, e)
		}
	}

	return nil
}
func (re *GroupByExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The reducer worker does not require client finishing handling")
}

func (re *GroupByExecutor) HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The reducer worker does not implement Task 1")
}

func (re *GroupByExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {

	panic("The reducer worker does not implement Task 2, but Task 2.1 and 2.2 separately")
}
