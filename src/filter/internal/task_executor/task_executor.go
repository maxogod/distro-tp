package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/filter/business"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type FilterExecutor struct {
	config          TaskConfig
	filterService   business.FilterService
	aggregatorQueue middleware.MessageMiddleware
	groupByQueue    middleware.MessageMiddleware
}

func NewFilterExecutor(config TaskConfig, filterService business.FilterService, groupByQueue middleware.MessageMiddleware, aggregatorQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &FilterExecutor{
		config:          config,
		filterService:   filterService,
		aggregatorQueue: aggregatorQueue,
		groupByQueue:    groupByQueue,
	}
}

func (fe *FilterExecutor) HandleTask1(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	fe.filterService.FilterByYear(transactionBatch)
	fe.filterService.FilterByTime(transactionBatch)
	fe.filterService.FilterByFinalAmount(transactionBatch)

	return worker.SendDataToMiddleware(transactionBatch, enum.T1, clientID, fe.aggregatorQueue)
}

func (fe *FilterExecutor) HandleTask2(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionItemsBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	fe.filterService.FilterItemsByYear(transactionBatch)

	return worker.SendDataToMiddleware(transactionBatch, enum.T2, clientID, fe.groupByQueue)
}

func (fe *FilterExecutor) HandleTask3(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}
	log.Debugf("Received %d transactions for Task 3 from client %s", len(transactionBatch.Transactions), clientID)

	fe.filterService.FilterByYear(transactionBatch)
	fe.filterService.FilterByTime(transactionBatch)

	return worker.SendDataToMiddleware(transactionBatch, enum.T3, clientID, fe.groupByQueue)
}

func (fe *FilterExecutor) HandleTask4(payload []byte, clientID string) error {
	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}
	log.Infof("Received %d transactions for Task 4 from client %s", len(transactionBatch.Transactions), clientID)

	return worker.SendDataToMiddleware(transactionBatch, enum.T4, clientID, fe.groupByQueue)
}

func (fe *FilterExecutor) Close() error {
	e := fe.aggregatorQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close aggregator queue: %v", e)
	}

	e = fe.groupByQueue.Close()

	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close group by queue: %v", e)
	}
	return nil
}

func (fe *FilterExecutor) HandleTask2_1(payload []byte, clientID string) error {
	panic("The filter does not implement Task 2.1")
}

func (fe *FilterExecutor) HandleTask2_2(payload []byte, clientID string) error {
	panic("The filter does not implement Task 2.2")
}

func (fe *FilterExecutor) HandleFinishClient(clientID string) error {
	panic("Filter does not require client finishing handling")
}
