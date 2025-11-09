package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/reducer/business"
	"google.golang.org/protobuf/proto"
)

type reducerExecutor struct {
	service          business.ReducerService
	url              string
	connectedClients map[string]middleware.MessageMiddleware
	outputQueue      middleware.MessageMiddleware
}

func NewReducerExecutor(filterService business.ReducerService,
	url string,
	connectedClients map[string]middleware.MessageMiddleware,
	reducerQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &reducerExecutor{
		service:          filterService,
		url:              url,
		connectedClients: connectedClients,
		outputQueue:      reducerQueue,
	}
}

func (re *reducerExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	groupedItems := &group_by.GroupTransactionItemsBatch{}
	reducedResult := &reduced.TotalSumItemsBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, groupedItems)
	if err != nil {
		return err
	}
	// === Business logic ===
	for _, group := range groupedItems.GetGroupTransactionItems() {
		reduced := re.service.SumTotalItems(group)
		reducedResult.TotalSumItems = append(reducedResult.GetTotalSumItems(), reduced)
	}

	err = worker.SendDataToMiddleware(reducedResult, enum.T2, clientID, re.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true

	_, exists := re.connectedClients[clientID]
	if !exists {
		re.connectedClients[clientID] = middleware.GetCounterExchange(re.url, clientID+"@"+string(enum.ReducerWorker))
	}
	counterExchange := re.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 1, enum.ReducerWorker, enum.JoinerWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (re *reducerExecutor) HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	groupTransactions := &group_by.GroupTransactionsBatch{}
	reducedBatch := &reduced.TotalPaymentValueBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, groupTransactions)
	if err != nil {
		return err
	}
	// === Business logic ===
	for _, group := range groupTransactions.GetGroupedTransactions() {
		reduced := re.service.SumTotalPaymentValue(group)
		reducedBatch.TotalPaymentValues = append(reducedBatch.GetTotalPaymentValues(), reduced)
	}

	err = worker.SendDataToMiddleware(reducedBatch, enum.T3, clientID, re.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	amountSent := 1
	shouldAck = true

	_, exists := re.connectedClients[clientID]
	if !exists {
		re.connectedClients[clientID] = middleware.GetCounterExchange(re.url, clientID+"@"+string(enum.ReducerWorker))
	}
	counterExchange := re.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, amountSent, enum.ReducerWorker, enum.JoinerWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (re *reducerExecutor) HandleTask4(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	groupTransactions := &group_by.GroupTransactionsBatch{}
	countedTransactionsBatch := &reduced.CountedUserTransactionBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, groupTransactions)
	if err != nil {
		return err
	}
	// === Business logic ===
	for _, group := range groupTransactions.GetGroupedTransactions() {
		countedTransactions := re.service.CountUserTransactions(group)
		countedTransactionsBatch.CountedUserTransactions = append(countedTransactionsBatch.GetCountedUserTransactions(), countedTransactions)
	}

	err = worker.SendDataToMiddleware(countedTransactionsBatch, enum.T4, clientID, re.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true

	_, exists := re.connectedClients[clientID]
	if !exists {
		re.connectedClients[clientID] = middleware.GetCounterExchange(re.url, clientID+"@"+string(enum.ReducerWorker))
	}
	counterExchange := re.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 1, enum.ReducerWorker, enum.JoinerWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (re *reducerExecutor) Close() error {
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
func (re *reducerExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The reducer worker does not require client finishing handling")
}

func (re *reducerExecutor) HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The reducer worker does not implement Task 1")
}

func (re *reducerExecutor) HandleTask2_1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {

	panic("THIS WILL BE GONE SOON")

}

func (re *reducerExecutor) HandleTask2_2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {

	panic("THIS WILL BE GONE SOON")
}
