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
	service     business.ReducerService
	outputQueue middleware.MessageMiddleware
}

func NewReducerExecutor(filterService business.ReducerService, reducerQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &GroupByExecutor{
		service:     filterService,
		outputQueue: reducerQueue,
	}
}

func (fe *GroupByExecutor) HandleTask2_1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
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
	reducedItems := fe.service.SumTotalProfitBySubtotal(groupedItems)

	err = worker.SendDataToMiddleware(reducedItems, enum.T2_1, clientID, fe.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true
	return nil
}

func (fe *GroupByExecutor) HandleTask2_2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
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
	reducedItems := fe.service.SumTotalSoldByQuantity(groupedItems)

	err = worker.SendDataToMiddleware(reducedItems, enum.T2_2, clientID, fe.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true
	return nil
}

func (fe *GroupByExecutor) HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
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
	reducedTransactions := fe.service.SumTotalPaymentValue(groupTransactions)

	err = worker.SendDataToMiddleware(reducedTransactions, enum.T3, clientID, fe.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true
	return nil
}

func (fe *GroupByExecutor) HandleTask4(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
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
	reducedTransactions := fe.service.CountUserTransactions(groupTransactions)

	err = worker.SendDataToMiddleware(reducedTransactions, enum.T4, clientID, fe.outputQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true
	return nil
}

func (fe *GroupByExecutor) Close() error {

	e := fe.outputQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close reducer queue: %v", e)
	}
	return nil
}
func (fe *GroupByExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The reducer worker does not require client finishing handling")
}

func (fe *GroupByExecutor) HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The reducer worker does not implement Task 1")
}

func (fe *GroupByExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {

	panic("The reducer worker does not implement Task 2, but Task 2.1 and 2.2 separately")
}
