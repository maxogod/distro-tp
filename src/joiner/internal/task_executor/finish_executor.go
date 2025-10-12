package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/joiner/business"
	"google.golang.org/protobuf/proto"
)

type finishExecutor struct {
	address           string
	aggregatorService business.JoinerService
	sortExecutors     map[enum.TaskType]func(clientID string) error
	finishExecutors   map[enum.TaskType]func(clientID string) error
}

func NewFinishExecutor(address string, aggregatorService business.JoinerService) FinishExecutor {
	fe := finishExecutor{
		address:           address,
		aggregatorService: aggregatorService,
	}

	fe.sortExecutors = map[enum.TaskType]func(clientID string) error{
		enum.T2: fe.sortTask2,
		enum.T3: fe.sortTask3,
		enum.T4: fe.sortTask4,
	}

	fe.finishExecutors = map[enum.TaskType]func(clientID string) error{
		enum.T1: fe.finishTask1,
		enum.T2: fe.finishTask2,
		enum.T3: fe.finishTask3,
		enum.T4: fe.finishTask4,
	}
	return &fe
}

func (fe *finishExecutor) SortTaskData(clientID string, taskType enum.TaskType) error {
	sortFunc, ok := fe.sortExecutors[taskType]
	if !ok {
		return fmt.Errorf("no sort executor found for task type: %v", taskType)
	}
	return sortFunc(clientID)
}

func (fe *finishExecutor) SendAllData(clientID string, taskType enum.TaskType) error {
	finishFunc, ok := fe.finishExecutors[taskType]
	if !ok {
		return fmt.Errorf("no finish executor found for task type: %v", taskType)
	}
	return finishFunc(clientID)
}

func (fe *finishExecutor) sortTask2(clientID string) error {
	// TODO: implement sortTask2
	return nil
}

func (fe *finishExecutor) sortTask3(clientID string) error {
	sortFn := func(a, b *proto.Message) bool {
		txA := (*a).(*reduced.TotalPaymentValue)
		txB := (*b).(*reduced.TotalPaymentValue)
		return txA.FinalAmount > txB.FinalAmount
	}
	return fe.aggregatorService.SortData(clientID, sortFn)
}

func (fe *finishExecutor) sortTask4(clientID string) error {
	// TODO: implement sortTask4
	return nil
}

func (fe *finishExecutor) finishTask1(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer processedDataQueue.Close()
	for {
		transactions, moreBatches := fe.aggregatorService.GetStoredTransactions(clientID, TRANSACTION_SEND_LIMIT)
		if !moreBatches {
			break
		}
		transactionBatch := &raw.TransactionBatch{
			Transactions: transactions,
		}
		if err := worker.SendDataToMiddleware(transactionBatch, enum.T1, clientID, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
	}
	return worker.SendDone(clientID, enum.T1, processedDataQueue)
}

func (fe *finishExecutor) finishTask2(clientID string) error {
	// TODO: implement finishTask2
	return nil
}

func (fe *finishExecutor) finishTask3(clientID string) error {

	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer processedDataQueue.Close()
	for {
		tpvDataBatch, moreBatches := fe.aggregatorService.GetStoredTotalPaymentValue(clientID, TRANSACTION_SEND_LIMIT)
		if !moreBatches {
			break
		}

		for _, tpvData := range tpvDataBatch {
			if err := worker.SendDataToMiddleware(tpvData, enum.T1, clientID, processedDataQueue); err != nil {
				return fmt.Errorf("failed to send data to middleware: %v", err)
			}
		}
	}
	return worker.SendDone(clientID, enum.T3, processedDataQueue)
}

func (fe *finishExecutor) finishTask4(clientID string) error {

	return nil
}
