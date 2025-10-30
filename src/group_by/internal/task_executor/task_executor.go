package task_executor

import (
	"fmt"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/group_by/business"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type groupData struct {
	data   []proto.Message
	isDone bool
}

type groupAccumulator struct {
	dataChan chan groupData
	timer    *time.Timer
}

type GroupExecutor struct {
	service          business.GroupService
	url              string
	connectedClients map[string]middleware.MessageMiddleware
	reducerQueue     middleware.MessageMiddleware
	groupAccumulator map[string]groupAccumulator
}

func NewGroupExecutor(groupService business.GroupService,
	url string,
	connectedClients map[string]middleware.MessageMiddleware,
	reducerQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &GroupExecutor{
		service:          groupService,
		url:              url,
		connectedClients: connectedClients,
		reducerQueue:     reducerQueue,
		groupAccumulator: make(map[string]groupAccumulator),
	}
}

func (ge *GroupExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	transactionBatch := &raw.TransactionItemsBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	// === Business logic ===
	groupedData := ge.service.GroupItemsByYearMonthAndItem(transactionBatch.GetTransactionItems())

	// This output is sent to both T2.1 and T2.2
	// So we iterate over the map and send each grouped data to both queues
	// This will increase the traffic twice as much as any other task
	amountSent := 0
	for _, group := range groupedData {
		err := worker.SendDataToMiddleware(group, enum.T2_1, clientID, ge.reducerQueue)
		if err != nil {
			shouldRequeue = true
			return err
		}
		err = worker.SendDataToMiddleware(group, enum.T2_2, clientID, ge.reducerQueue)
		if err != nil {
			shouldRequeue = true
			return err
		}
		amountSent += 2
	}
	shouldAck = true

	_, exists := ge.connectedClients[clientID]
	if !exists {
		ge.connectedClients[clientID] = middleware.GetCounterExchange(ge.url, clientID+"@"+string(enum.GroupbyWorker))
	}
	counterExchange := ge.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, amountSent, enum.GroupbyWorker, enum.ReducerWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (ge *GroupExecutor) HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	transactionBatch := &raw.TransactionBatch{}
	groupBatch := &group_by.GroupTransactionsBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	// === Business logic ===
	groupedData := ge.service.GroupTransactionsByStoreAndSemester(transactionBatch.GetTransactions())
	for _, group := range groupedData {
		groupBatch.GroupedTransactions = append(groupBatch.GetGroupedTransactions(), group)
	}

	err = worker.SendDataToMiddleware(groupBatch, enum.T3, clientID, ge.reducerQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true

	_, exists := ge.connectedClients[clientID]
	if !exists {
		ge.connectedClients[clientID] = middleware.GetCounterExchange(ge.url, clientID+"@"+string(enum.GroupbyWorker))
	}
	counterExchange := ge.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 1, enum.GroupbyWorker, enum.ReducerWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (ge *GroupExecutor) HandleTask4(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	transactionBatch := &raw.TransactionBatch{}
	groupTransactionBatch := &group_by.GroupTransactionsBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	// === Business logic ===
	groupedData := ge.service.GroupTransactionsByStoreAndUser(transactionBatch.GetTransactions())
	for _, group := range groupedData {
		groupTransactionBatch.GroupedTransactions = append(groupTransactionBatch.GetGroupedTransactions(), group)
	}

	err = worker.SendDataToMiddleware(groupTransactionBatch, enum.T4, clientID, ge.reducerQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true

	_, exists := ge.connectedClients[clientID]
	if !exists {
		ge.connectedClients[clientID] = middleware.GetCounterExchange(ge.url, clientID+"@"+string(enum.GroupbyWorker))
	}
	counterExchange := ge.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 1, enum.GroupbyWorker, enum.ReducerWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (ge *GroupExecutor) Close() error {
	if e := ge.reducerQueue.Close(); e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close reducer queue: %v", e)
	}

	for clientID, q := range ge.connectedClients {
		if e := q.Close(); e != middleware.MessageMiddlewareSuccess {
			log.Errorf("failed to close middleware for client %s: %v", clientID, e)
		}
	}
	return nil
}

func (ge *GroupExecutor) HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The group by worker does not implement Task 1")
}

func (ge *GroupExecutor) HandleTask2_1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The group by worker does not implement Task 2.1")
}

func (ge *GroupExecutor) HandleTask2_2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The group by worker does not implement Task 2.2")
}

// TODO: handle this to remove the client after finishing
func (ge *GroupExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The group by worker does not require client finishing handling")
}
