package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/worker"
)

type finishExecutor struct {
	address           string
	aggregatorService business.AggregatorService
	sortExecutors     map[enum.TaskType]func(clientID string) error
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
		enum.T1:   fe.finishTask1,
		enum.T2_1: fe.finishTask2_1,
		enum.T2_2: fe.finishTask2_2,
		enum.T3:   fe.finishTask3,
		enum.T4:   fe.finishTask4,
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
	defer processedDataQueue.Close()

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
		batch := results[index:endIndex]
		for _, transaction := range batch {
			if err := worker.SendDataToMiddleware(transaction, enum.T1, clientID, processedDataQueue); err != nil {
				return fmt.Errorf("failed to send data to middleware: %v", err)
			}
		}
		index = endIndex
	}

	return worker.SendDone(clientID, enum.T1, processedDataQueue)
}

func (fe *finishExecutor) finishTask2_1(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer processedDataQueue.Close()
	clientWithPrefix := T2_1_PREFIX + clientID
	results, err := fe.aggregatorService.GetStoredTotalProfitBySubtotal(clientWithPrefix)
	if err != nil {
		return fmt.Errorf("[TASK 2.1] failed to get results for client %s: %v", clientWithPrefix, err)
	}
	for _, tpsData := range results {
		if err := worker.SendDataToMiddleware(tpsData, enum.T2_1, clientID, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
	}
	return worker.SendDone(clientID, enum.T2_1, processedDataQueue)
}

func (fe *finishExecutor) finishTask2_2(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer processedDataQueue.Close()
	clientWithPrefix := T2_2_PREFIX + clientID
	results, err := fe.aggregatorService.GetStoredTotalSoldByQuantity(clientWithPrefix)
	if err != nil {
		return fmt.Errorf("[TASK 2.2] failed to get results for client %s: %v", clientWithPrefix, err)
	}
	for _, tsqData := range results {
		if err := worker.SendDataToMiddleware(tsqData, enum.T2_2, clientID, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
	}
	return worker.SendDone(clientID, enum.T2_2, processedDataQueue)
}

func (fe *finishExecutor) finishTask3(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer processedDataQueue.Close()
	clientWithPrefix := T2_1_PREFIX + clientID
	results, err := fe.aggregatorService.GetStoredTotalPaymentValue(clientWithPrefix)
	if err != nil {
		return fmt.Errorf("[TASK 3] failed to get results for client %s: %v", clientWithPrefix, err)
	}

	for _, tpvData := range results {
		if err := worker.SendDataToMiddleware(tpvData, enum.T3, clientID, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
	}
	return worker.SendDone(clientID, enum.T3, processedDataQueue)
}

func (fe *finishExecutor) finishTask4(clientID string) error {

	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer processedDataQueue.Close()
	topUsersPerStore, err := fe.aggregatorService.GetStoredCountedUserTransactions(clientID)
	if err != nil {
		return fmt.Errorf("[TASK 4] failed to get results for client %s: %v", clientID, err)
	}
	for _, orderedList := range topUsersPerStore {
		amountToSend := min(len(orderedList), fe.limits.MaxAmountToSend)
		for _, user := range orderedList[:amountToSend] {
			if err := worker.SendDataToMiddleware(user, enum.T4, clientID, processedDataQueue); err != nil {
				return fmt.Errorf("failed to send data for store %s: %v", user.GetStoreId(), err)
			}
		}
	}
	return worker.SendDone(clientID, enum.T4, processedDataQueue)
}
