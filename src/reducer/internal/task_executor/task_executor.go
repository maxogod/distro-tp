package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/group_by"
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

func (fe *GroupByExecutor) HandleTask2_1(payload []byte, clientID string) error {
	groupedItems := &group_by.GroupTransactionItems{}
	err := proto.Unmarshal(payload, groupedItems)
	if err != nil {
		return err
	}
	// === Business logic ===
	reducedItems := fe.service.SumTotalProfitBySubtotal(groupedItems)

	return worker.SendDataToMiddleware(reducedItems, enum.T2_1, clientID, fe.outputQueue)
}

func (fe *GroupByExecutor) HandleTask2_2(payload []byte, clientID string) error {
	groupedItems := &group_by.GroupTransactionItems{}
	err := proto.Unmarshal(payload, groupedItems)
	if err != nil {
		return err
	}
	// === Business logic ===
	reducedItems := fe.service.SumTotalSoldByQuantity(groupedItems)

	return worker.SendDataToMiddleware(reducedItems, enum.T2_2, clientID, fe.outputQueue)

}

func (fe *GroupByExecutor) HandleTask3(payload []byte, clientID string) error {
	groupTransactions := &group_by.GroupTransactions{}
	err := proto.Unmarshal(payload, groupTransactions)
	if err != nil {
		return err
	}
	// === Business logic ===
	reducedTransactions := fe.service.SumTotalPaymentValue(groupTransactions)

	return worker.SendDataToMiddleware(reducedTransactions, enum.T3, clientID, fe.outputQueue)
}

func (fe *GroupByExecutor) HandleTask4(payload []byte, clientID string) error {
	groupTransactions := &group_by.GroupTransactions{}
	err := proto.Unmarshal(payload, groupTransactions)
	if err != nil {
		return err
	}
	// === Business logic ===
	reducedTransactions := fe.service.CountUserTransactions(groupTransactions)

	return worker.SendDataToMiddleware(reducedTransactions, enum.T4, clientID, fe.outputQueue)
}

func (fe *GroupByExecutor) Close() error {

	e := fe.outputQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close reducer queue: %v", e)
	}
	return nil
}
func (fe *GroupByExecutor) HandleFinishClient(clientID string) error {
	panic("The reducer worker does not require client finishing handling")
}

func (fe *GroupByExecutor) HandleTask1(payload []byte, clientID string) error {
	panic("The reducer worker does not implement Task 1")
}

func (fe *GroupByExecutor) HandleTask2(payload []byte, clientID string) error {

	panic("The reducer worker does not implement Task 2, but Task 2.1 and 2.2 separately")
}
