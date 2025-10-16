package task_executor

import (
	"fmt"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/group_by/business"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

const FLUSH_TIMEOUT = 500 * time.Millisecond

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
	reducerQueue     middleware.MessageMiddleware
	groupAccumulator map[string]groupAccumulator
}

func NewGroupExecutor(groupService business.GroupService, reducerQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &GroupExecutor{
		service:          groupService,
		reducerQueue:     reducerQueue,
		groupAccumulator: make(map[string]groupAccumulator),
	}
}

func (fe *GroupExecutor) HandleTask2(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionItemsBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}
	flushFn := func(id string, flushed []proto.Message) error {
		if len(flushed) == 0 {
			return nil
		}
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
	fe.initGroupFlusher(clientID, flushFn)
	protoData := make([]proto.Message, len(transactionBatch.GetTransactionItems()))
	for i, item := range transactionBatch.GetTransactionItems() {
		protoData[i] = item
	}
	fe.AccumulateGroups(protoData, clientID)
	return nil
}

func (fe *GroupExecutor) HandleTask3(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	flushFn := func(id string, flushed []proto.Message) error {
		if len(flushed) == 0 {
			return nil
		}
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

	fe.initGroupFlusher(clientID, flushFn)

	protoData := make([]proto.Message, len(transactionBatch.GetTransactions()))
	for i, item := range transactionBatch.GetTransactions() {
		protoData[i] = item
	}
	fe.AccumulateGroups(protoData, clientID)
	return nil
}

func (fe *GroupExecutor) HandleTask4(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	flushFn := func(id string, flushed []proto.Message) error {
		if len(flushed) == 0 {
			return nil
		}
		transactions := make([]*raw.Transaction, len(flushed))
		for i, msg := range flushed {
			transactions[i] = msg.(*raw.Transaction)
		}

		// === Business logic ===
		groupedData := fe.service.GroupTransactionsByStoreAndUser(transactions)

		for _, group := range groupedData {
			err := worker.SendDataToMiddleware(group, enum.T4, clientID, fe.reducerQueue)
			if err != nil {
				return err
			}
		}
		return nil
	}

	fe.initGroupFlusher(clientID, flushFn)

	protoData := make([]proto.Message, len(transactionBatch.GetTransactions()))
	for i, item := range transactionBatch.GetTransactions() {
		protoData[i] = item
	}
	fe.AccumulateGroups(protoData, clientID)
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

// TODO: handle this to remove the client after finishing
func (fe *GroupExecutor) HandleFinishClient(clientID string) error {
	panic("The group by worker does not require client finishing handling")
}

func (fe *GroupExecutor) AccumulateGroups(data []proto.Message, clientID string) error {
	// Get or create the accumulator for this client
	acc, exists := fe.groupAccumulator[clientID]

	if !exists {
		return fmt.Errorf("no accumulator found for client %s", clientID)
	}

	acc.dataChan <- groupData{data: data, isDone: false}
	return nil
}

func (fe *GroupExecutor) initGroupFlusher(clientID string, flushFn func(string, []proto.Message) error) {

	// This method should actually be called once, so th handle it we use this
	_, exists := fe.groupAccumulator[clientID]
	if exists {
		return
	}

	acc := groupAccumulator{
		dataChan: make(chan groupData),
		timer:    nil,
	}
	acc.timer = time.AfterFunc(FLUSH_TIMEOUT, func() {
		acc.dataChan <- groupData{data: nil, isDone: true}
		acc.timer.Reset(FLUSH_TIMEOUT)
	})
	fe.groupAccumulator[clientID] = acc

	go func() {
		flushData := []proto.Message{}
		for {
			grp := <-acc.dataChan
			if grp.isDone {
				err := flushFn(clientID, flushData)
				if err != nil {
					log.Debugf("failed to flush data for client %s: %v", clientID, err)
				}
				flushData = []proto.Message{}
			}
			flushData = append(flushData, grp.data...)
		}
	}()
}
