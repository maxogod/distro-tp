package task_executor

import (
	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

type finishExecutor struct {
	aggregatorService business.AggregatorService
	sortExecutors     map[enum.TaskType]func(clientID string) error
	finishExecutors   map[enum.TaskType]func(clientID string, processedDataQueue middleware.MessageMiddleware) error
}

func NewFinishExecutor(aggregatorService business.AggregatorService) FinishExecutor {
	fe := finishExecutor{
		aggregatorService: aggregatorService,
	}

	fe.sortExecutors = map[enum.TaskType]func(clientID string) error{
		enum.T2: fe.sortTask2,
		enum.T3: fe.sortTask3,
		enum.T4: fe.sortTask4,
	}

	fe.finishExecutors = map[enum.TaskType]func(clientID string, processedDataQueue middleware.MessageMiddleware) error{
		enum.T2: fe.finishTask2,
		enum.T3: fe.finishTask3,
		enum.T4: fe.finishTask4,
	}
	return &fe
}

func (fe *finishExecutor) SortTaskData(clientID string, taskType enum.TaskType) error {
	return nil
}

func (fe *finishExecutor) SendAllData(clientID string, taskType enum.TaskType) error {
	return nil
}

func (fe *finishExecutor) sortTask2(clientID string) error {

	return nil
}

func (fe *finishExecutor) sortTask3(clientID string) error {

	return nil
}

func (fe *finishExecutor) sortTask4(clientID string) error {

	return nil
}

func (fe *finishExecutor) finishTask2(clientID string, processedDataQueue middleware.MessageMiddleware) error {

	return nil
}

func (fe *finishExecutor) finishTask3(clientID string, processedDataQueue middleware.MessageMiddleware) error {

	return nil
}

func (fe *finishExecutor) finishTask4(clientID string, processedDataQueue middleware.MessageMiddleware) error {

	return nil
}

// ================================
// add this logic in the finishTask
// ================================
// func (ae *AggregatorExecutor) sendAllTransactions(clientID string, processedDataQueue middleware.MessageMiddleware) error {
// 	for {
// 		transactions, moreBatches := ae.aggregatorService.GetStoredTransactions(clientID, TRANSACTION_SEND_LIMIT)
// 		if !moreBatches {
// 			break
// 		}
// 		transactionBatch := &raw.TransactionBatch{
// 			Transactions: transactions,
// 		}
// 		if err := worker.SendDataToMiddleware(transactionBatch, enum.T1, clientID, processedDataQueue); err != nil {
// 			return fmt.Errorf("failed to send data to middleware: %v", err)
// 		}
// 	}
// 	return nil
// }
// ===================================================
// Then, use the sendDOne after sending all of the data
// ===================================================
// err = worker.SendDone(clientID, processedDataQueue)
// if err != nil {
// 	return fmt.Errorf("failed to send done message to middleware: %v", err)
// }
