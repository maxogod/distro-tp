package task_executor

import (
	"fmt"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/group_by/business"
	"google.golang.org/protobuf/proto"
)

const FLUSH_TIMEOUT = 3 * time.Second
const MAX_ACCUMULATE = 1000

type groupAcumulator struct {
	data  []proto.Message
	timer *time.Timer
}

type GroupExecutor struct {
	service          business.GroupService
	reducerQueue     middleware.MessageMiddleware
	groupAccumulator map[string]groupAcumulator
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

	flushFn := func(id string) error {
		flushed := fe.FlushGroups(id)
		transactionItems := make([]*raw.TransactionItem, len(flushed))
		for i, msg := range flushed {
			transactionItems[i] = msg.(*raw.TransactionItem)
		}
		// === Business logic ===
		groupedData := fe.service.GroupItemsByYearMonthAndItem(transactionItems)

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
	protoData := make([]proto.Message, len(transactionBatch.GetTransactionItems()))
	for i, item := range transactionBatch.GetTransactionItems() {
		protoData[i] = item
	}
	fe.AccumulateGroups(protoData, clientID, flushFn)
	return nil
}

func (fe *GroupExecutor) HandleTask3(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	flushFn := func(id string) error {
		flushed := fe.FlushGroups(id)
		transactions := make([]*raw.Transaction, len(flushed))
		for i, msg := range flushed {
			transactions[i] = msg.(*raw.Transaction)
		}
		// === Business logic ===
		groupedData := fe.service.GroupTransactionsByStoreAndSemester(transactions)

		for _, group := range groupedData {
			err := worker.SendDataToMiddleware(group, enum.T3, clientID, fe.reducerQueue)
			if err != nil {
				return err
			}
		}
		return nil
	}
	protoData := make([]proto.Message, len(transactionBatch.GetTransactions()))
	for i, item := range transactionBatch.GetTransactions() {
		protoData[i] = item
	}
	fe.AccumulateGroups(protoData, clientID, flushFn)
	return nil
}

func (fe *GroupExecutor) HandleTask4(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	flushFn := func(id string) error {
		flushed := fe.FlushGroups(id)
		transactions := make([]*raw.Transaction, len(flushed))
		for i, msg := range flushed {
			transactions[i] = msg.(*raw.Transaction)
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
	protoData := make([]proto.Message, len(transactionBatch.GetTransactions()))
	for i, item := range transactionBatch.GetTransactions() {
		protoData[i] = item
	}
	fe.AccumulateGroups(protoData, clientID, flushFn)
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

func (fe *GroupExecutor) AccumulateGroups(data []proto.Message, clientID string, flushFn func(string) error) error {
	if fe.groupAccumulator == nil {
		fe.groupAccumulator = make(map[string]groupAcumulator)
	}

	// Get or create the accumulator for this client
	acc, exists := fe.groupAccumulator[clientID]
	if !exists {
		acc = groupAcumulator{
			data:  []proto.Message{},
			timer: nil,
		}
	}

	// Append new data
	acc.data = append(acc.data, data...)

	// If a timer exists, stop it before starting a new one
	if acc.timer != nil {
		acc.timer.Stop()
	}

	if len(acc.data) >= MAX_ACCUMULATE {
		e := flushFn(clientID)
		if e != nil {
			return e
		}
		return nil
	}

	// Start a new timer
	acc.timer = time.AfterFunc(FLUSH_TIMEOUT, func() {
		e := flushFn(clientID)
		if e != nil {
			fmt.Printf("Error flushing groups for client %s: %v\n", clientID, e)
		}
	})

	fe.groupAccumulator[clientID] = acc
	return nil
}

func (fe *GroupExecutor) FlushGroups(clientID string) []proto.Message {
	acc, exists := fe.groupAccumulator[clientID]
	if !exists {
		return nil
	}
	delete(fe.groupAccumulator, clientID)
	return acc.data
}
