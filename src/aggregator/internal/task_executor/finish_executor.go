package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
	"google.golang.org/protobuf/proto"
)

type finishExecutor struct {
	address           string
	aggregatorService business.AggregatorService
	sortExecutors     map[enum.TaskType]func(clientID string) error
	finishExecutors   map[enum.TaskType]func(clientID string) error
}

func NewFinishExecutor(address string, aggregatorService business.AggregatorService) FinishExecutor {
	fe := finishExecutor{
		address:           address,
		aggregatorService: aggregatorService,
	}

	fe.sortExecutors = map[enum.TaskType]func(clientID string) error{
		enum.T2_1: fe.sortTask2_1,
		enum.T2_2: fe.sortTask2_2,
		enum.T3:   fe.sortTask3,
		enum.T4:   fe.sortTask4,
	}

	fe.finishExecutors = map[enum.TaskType]func(clientID string) error{
		enum.T1:   fe.finishTask1,
		enum.T2_1: fe.finishTask2_1,
		enum.T2_2: fe.finishTask2_2,
		enum.T3:   fe.finishTask3,
		enum.T4:   fe.finishTask4,
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

func (fe *finishExecutor) sortTask2_1(clientID string) error {
	sortFn := func(a, b *proto.Message) bool {
		txA := (*a).(*reduced.TotalProfitBySubtotal)
		txB := (*b).(*reduced.TotalProfitBySubtotal)
		return txA.GetSubtotal() > txB.GetSubtotal()
	}
	return fe.aggregatorService.SortData(clientID, sortFn)
}

func (fe *finishExecutor) sortTask2_2(clientID string) error {
	sortFn := func(a, b *proto.Message) bool {
		txA := (*a).(*reduced.TotalSoldByQuantity)
		txB := (*b).(*reduced.TotalSoldByQuantity)
		return txA.GetQuantity() > txB.GetQuantity()
	}
	return fe.aggregatorService.SortData(clientID, sortFn)
}

func (fe *finishExecutor) sortTask3(clientID string) error {
	sortFn := func(a, b *proto.Message) bool {
		txA := (*a).(*reduced.TotalPaymentValue)
		txB := (*b).(*reduced.TotalPaymentValue)
		return txA.GetFinalAmount() > txB.GetFinalAmount()
	}
	return fe.aggregatorService.SortData(clientID, sortFn)
}

func (fe *finishExecutor) sortTask4(clientID string) error {
	sortFn := func(a, b *proto.Message) bool {
		txA := (*a).(*reduced.CountedUserTransactions)
		txB := (*b).(*reduced.CountedUserTransactions)
		return txA.GetTransactionQuantity() > txB.GetTransactionQuantity()
	}
	return fe.aggregatorService.SortData(clientID, sortFn)
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
	return worker.SendDone(clientID, processedDataQueue)
}

func (fe *finishExecutor) finishTask2_1(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer processedDataQueue.Close()
	for {
		tpvDataBatch, moreBatches := fe.aggregatorService.GetStoredTotalProfitBySubtotal(clientID, TRANSACTION_SEND_LIMIT)
		if !moreBatches {
			break
		}

		for _, tpvData := range tpvDataBatch {
			if err := worker.SendDataToMiddleware(tpvData, enum.T1, clientID, processedDataQueue); err != nil {
				return fmt.Errorf("failed to send data to middleware: %v", err)
			}
		}
	}
	return worker.SendDone(clientID, processedDataQueue)
}

func (fe *finishExecutor) finishTask2_2(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer processedDataQueue.Close()
	for {
		tpvDataBatch, moreBatches := fe.aggregatorService.GetStoredTotalSoldByQuantity(clientID, TRANSACTION_SEND_LIMIT)
		if !moreBatches {
			break
		}

		for _, tpvData := range tpvDataBatch {
			if err := worker.SendDataToMiddleware(tpvData, enum.T1, clientID, processedDataQueue); err != nil {
				return fmt.Errorf("failed to send data to middleware: %v", err)
			}
		}
	}
	return worker.SendDone(clientID, processedDataQueue)
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
	return worker.SendDone(clientID, processedDataQueue)
}

func (fe *finishExecutor) finishTask4(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer processedDataQueue.Close()
	for {
		countedTransactions, moreBatches := fe.aggregatorService.GetStoredCountedUserTransactions(clientID, TRANSACTION_SEND_LIMIT)
		if !moreBatches {
			break
		}

		for _, countedData := range countedTransactions {
			if err := worker.SendDataToMiddleware(countedData, enum.T1, clientID, processedDataQueue); err != nil {
				return fmt.Errorf("failed to send data to middleware: %v", err)
			}
		}
	}
	return worker.SendDone(clientID, processedDataQueue)
}
