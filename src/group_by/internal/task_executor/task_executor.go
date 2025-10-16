package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/group_by/business"
	"google.golang.org/protobuf/proto"
)

type GroupExecutor struct {
	service      business.GroupService
	reducerQueue middleware.MessageMiddleware
}

func NewGroupExecutor(filterService business.GroupService, reducerQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &GroupExecutor{
		service:      filterService,
		reducerQueue: reducerQueue,
	}
}

func (fe *GroupExecutor) HandleTask2(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionItemsBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}
	// === Business logic ===
	groupedData := fe.service.GroupItemsByYearMonthAndItem(transactionBatch.GetTransactionItems())

	// This output is sent to both T2.1 and T2.2
	// So we iterate over the map and send each grouped data to both queues
	// This will increase the traffic twice as much as any other task
	for _, group := range groupedData {
		err := worker.SendDataToMiddleware(group, enum.T2_1, clientID, fe.reducerQueue)
		if err != nil {
			return err
		}
		err = worker.SendDataToMiddleware(group, enum.T2_2, clientID, fe.reducerQueue)
		if err != nil {
			return err
		}

	}
	return nil
}

func (fe *GroupExecutor) HandleTask3(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}
	// === Business logic ===
	groupedData := fe.service.GroupTransactionsByStoreAndSemester(transactionBatch.GetTransactions())

	for _, group := range groupedData {
		err := worker.SendDataToMiddleware(group, enum.T3, clientID, fe.reducerQueue)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fe *GroupExecutor) HandleTask4(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}
	// === Business logic ===
	groupedData := fe.service.GroupTransactionsByStoreAndUser(transactionBatch.GetTransactions())

	for _, group := range groupedData {
		err := worker.SendDataToMiddleware(group, enum.T4, clientID, fe.reducerQueue)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fe *GroupExecutor) Close() error {

	e := fe.reducerQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close reducer queue: %v", e)
	}
	return nil
}

func (fe *GroupExecutor) HandleTask1(payload []byte, clientID string) error {
	panic("The group by worker does not implement Task 1")
}

func (fe *GroupExecutor) HandleTask2_1(payload []byte, clientID string) error {
	panic("The group by worker does not implement Task 2.1")
}

func (fe *GroupExecutor) HandleTask2_2(payload []byte, clientID string) error {
	panic("The group by worker does not implement Task 2.2")
}

func (fe *GroupExecutor) HandleFinishClient(clientID string) error {
	panic("The group by worker does not require client finishing handling")
}
