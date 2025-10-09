package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/reducer/business"
	"google.golang.org/protobuf/proto"
)

type GroupByExecutor struct {
	service      business.ReducerService
	reducerQueue middleware.MessageMiddleware
}

func NewReducerExecutor(filterService business.ReducerService, reducerQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &GroupByExecutor{
		service:      filterService,
		reducerQueue: reducerQueue,
	}
}

func (fe *GroupByExecutor) HandleTask1(payload []byte, clientID string) error {

	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	//==========================
	// TODO: APPLY BUSINESS LOGIC HERE
	//==========================

	return worker.SendDataToMiddleware(transactionBatch, enum.T1, clientID, fe.reducerQueue)
}

func (fe *GroupByExecutor) HandleTask2(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionItemsBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	//==========================
	// TODO: APPLY BUSINESS LOGIC HERE
	//==========================

	return worker.SendDataToMiddleware(transactionBatch, enum.T3, clientID, fe.reducerQueue)
}

func (fe *GroupByExecutor) HandleTask3(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}
	//==========================
	// TODO: APPLY BUSINESS LOGIC HERE
	//==========================

	return worker.SendDataToMiddleware(transactionBatch, enum.T3, clientID, fe.reducerQueue)
}

func (fe *GroupByExecutor) HandleTask4(payload []byte, clientID string) error {
	// TODO: implement task 4 handling logic
	return nil
}

func (fe *GroupByExecutor) Close() error {

	e := fe.reducerQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close reducer queue: %v", e)
	}
	return nil
}

func (fe *GroupByExecutor) HandleTask2_1(payload []byte, clientID string) error {
	panic("The reducer worker does not implement Task 2.1")
}

func (fe *GroupByExecutor) HandleTask2_2(payload []byte, clientID string) error {
	panic("The reducer worker does not implement Task 2.2")
}

func (fe *GroupByExecutor) HandleFinishClient(clientID string) error {
	panic("The reducer worker does not require client finishing handling")
}
