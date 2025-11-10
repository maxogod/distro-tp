package task_executor

import (
	"fmt"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/group_by/business"
	"google.golang.org/protobuf/proto"
)

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
	reducerQueue     middleware.MessageMiddleware
	groupAccumulator map[string]groupAccumulator
}

func NewGroupExecutor(groupService business.GroupService,
	url string,
	reducerQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &GroupExecutor{
		service:          groupService,
		url:              url,
		reducerQueue:     reducerQueue,
		groupAccumulator: make(map[string]groupAccumulator),
	}
}

func (ge *GroupExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	transactionBatch := &raw.TransactionItemsBatch{}
	groupBatch := &group_by.GroupTransactionItemsBatch{}
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()

	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	// === Business logic ===
	groupedData := ge.service.GroupItemsByYearMonthAndItem(transactionBatch.GetTransactionItems())
	for _, group := range groupedData {
		groupBatch.GroupTransactionItems = append(groupBatch.GetGroupTransactionItems(), group)
	}

	err = worker.SendDataToMiddleware(groupBatch, enum.T2, clientID, int(dataEnvelope.GetSequenceNumber()), ge.reducerQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true
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

	err = worker.SendDataToMiddleware(groupBatch, enum.T3, clientID, int(dataEnvelope.GetSequenceNumber()), ge.reducerQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true
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

	err = worker.SendDataToMiddleware(groupTransactionBatch, enum.T4, clientID, int(dataEnvelope.GetSequenceNumber()), ge.reducerQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true
	return nil
}

func (ge *GroupExecutor) Close() error {
	if e := ge.reducerQueue.Close(); e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close reducer queue: %v", e)
	}
	return nil
}

func (ge *GroupExecutor) HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The group by worker does not implement Task 1")
}

// TODO: handle this to remove the client after finishing
func (ge *GroupExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The group by worker does not require client finishing handling")
}
