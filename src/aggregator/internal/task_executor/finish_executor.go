package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker"
)

type finishExecutor struct {
	address           string
	aggregatorService business.AggregatorService
	finishExecutors   map[enum.TaskType]func(clientID string) error
	limits            config.Limits
}

func NewFinishExecutor(address string, aggregatorService business.AggregatorService, limits config.Limits) FinishExecutor {
	fe := finishExecutor{
		address:           address,
		aggregatorService: aggregatorService,
		limits:            limits,
	}

	fe.finishExecutors = map[enum.TaskType]func(clientID string) error{
		enum.T1: fe.finishTask1,
		enum.T2: fe.finishTask2,
		enum.T3: fe.finishTask3,
		enum.T4: fe.finishTask4,
	}
	return &fe
}

func (fe *finishExecutor) SendAllData(clientID string, taskType enum.TaskType) error {
	finishFunc, ok := fe.finishExecutors[taskType]
	if !ok {
		return fmt.Errorf("no finish executor found for task type: %v", taskType)
	}

	return finishFunc(clientID)
}

func (fe *finishExecutor) finishTask1(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer func() {
		processedDataQueue.Close()
		fe.aggregatorService.RemoveData(clientID)
	}()

	results, err := fe.aggregatorService.GetStoredTransactions(clientID)
	if err != nil {
		return fmt.Errorf("[TASK 1] failed to get results for client %s: %v", clientID, err)
	}
	index := 0
	lenResults := len(results)
	for index < lenResults {
		endIndex := index + int(fe.limits.TransactionSendLimit)
		if endIndex > lenResults {
			endIndex = lenResults
		}
		transactionBatch := &raw.TransactionBatch{
			Transactions: results[index:endIndex],
		}
		if err := worker.SendDataToMiddleware(transactionBatch, enum.T1, clientID, 0, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
		index = endIndex
	}

	return worker.SendDone(clientID, enum.T1, processedDataQueue)
}

func (fe *finishExecutor) finishTask2(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer func() {
		processedDataQueue.Close()
		fe.aggregatorService.RemoveData(clientID)
	}()
	subtotalResults, quantityResults, err := fe.aggregatorService.GetStoredTotalItems(clientID)
	if err != nil {
		return fmt.Errorf("[TASK 2] failed to get results for client %s: %v", clientID, err)
	}
	for _, totalSubtotalData := range subtotalResults {
		if err := worker.SendDataToMiddleware(totalSubtotalData, enum.T2_1, clientID, 0, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
	}
	worker.SendDone(clientID, enum.T2_1, processedDataQueue)

	for _, totalQuantityData := range quantityResults {
		if err := worker.SendDataToMiddleware(totalQuantityData, enum.T2_2, clientID, 0, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
	}
	return worker.SendDone(clientID, enum.T2_2, processedDataQueue)
}

func (fe *finishExecutor) finishTask3(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer func() {
		processedDataQueue.Close()
		fe.aggregatorService.RemoveData(clientID)
	}()
	results, err := fe.aggregatorService.GetStoredTotalPaymentValue(clientID)
	if err != nil {
		return fmt.Errorf("[TASK 3] failed to get results for client %s: %v", clientID, err)
	}

	for _, tpvData := range results {
		if err := worker.SendDataToMiddleware(tpvData, enum.T3, clientID, 0, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
	}
	return worker.SendDone(clientID, enum.T3, processedDataQueue)
}

func (fe *finishExecutor) finishTask4(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer func() {
		processedDataQueue.Close()
		fe.aggregatorService.RemoveData(clientID)
	}()
	topUsersPerStore, err := fe.aggregatorService.GetStoredCountedUserTransactions(clientID)
	if err != nil {
		return fmt.Errorf("[TASK 4] failed to get results for client %s: %v", clientID, err)
	}
	for _, orderedList := range topUsersPerStore {
		amountToSend := min(len(orderedList), fe.limits.MaxAmountToSend)
		for _, user := range orderedList[:amountToSend] {
			if err := worker.SendDataToMiddleware(user, enum.T4, clientID, 0, processedDataQueue); err != nil {
				return fmt.Errorf("failed to send data for store %s: %v", user.GetStoreId(), err)
			}
		}
	}
	return worker.SendDone(clientID, enum.T4, processedDataQueue)
}
