package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
)

type finishExecutor struct {
	address           string
	aggregatorService business.AggregatorService
	finishExecutors   map[enum.TaskType]func(clientID string) error
	outputQueue       middleware.MessageMiddleware
	limits            config.Limits
}

func NewFinishExecutor(address string, aggregatorService business.AggregatorService, outputQueue middleware.MessageMiddleware, limits config.Limits) FinishExecutor {
	fe := finishExecutor{
		address:           address,
		aggregatorService: aggregatorService,
		outputQueue:       outputQueue,
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
	defer func() {
		fe.aggregatorService.RemoveData(clientID)
	}()
	subtotalResults, quantityResults, err := fe.aggregatorService.GetStoredTotalItems(clientID)
	if err != nil {
		return fmt.Errorf("[TASK 2] failed to get results for client %s: %v", clientID, err)
	}

	reportData := &reduced.TotalSumItemsReport{
		TotalSumItemsBySubtotal: subtotalResults,
		TotalSumItemsByQuantity: quantityResults,
	}
	// we send 1 message to the joiner
	return worker.SendDataToMiddleware(reportData, enum.T2, clientID, 0, fe.outputQueue)
}

func (fe *finishExecutor) finishTask3(clientID string) error {
	defer func() {
		fe.aggregatorService.RemoveData(clientID)
	}()
	results, err := fe.aggregatorService.GetStoredTotalPaymentValue(clientID)
	if err != nil {
		return fmt.Errorf("[TASK 3] failed to get results for client %s: %v", clientID, err)
	}

	reportData := &reduced.TotalPaymentValueBatch{
		TotalPaymentValues: results,
	}
	// we send 1 message to the joiner
	return worker.SendDataToMiddleware(reportData, enum.T3, clientID, 0, fe.outputQueue)
}

func (fe *finishExecutor) finishTask4(clientID string) error {
	defer func() {
		fe.aggregatorService.RemoveData(clientID)
	}()
	topUsersPerStore, err := fe.aggregatorService.GetStoredCountedUserTransactions(clientID)
	if err != nil {
		return fmt.Errorf("[TASK 4] failed to get results for client %s: %v", clientID, err)
	}

	topUsersResult := make([]*reduced.CountedUserTransactions, 0)

	// we get the top N users per store
	for _, orderedList := range topUsersPerStore {
		amountToSend := min(len(orderedList), fe.limits.MaxAmountToSend)
		topUsersResult = append(topUsersResult, orderedList[:amountToSend]...)
	}

	reportData := &reduced.CountedUserTransactionBatch{
		CountedUserTransactions: topUsersResult,
	}
	// we send 1 message to the joiner
	return worker.SendDataToMiddleware(reportData, enum.T4, clientID, 0, fe.outputQueue)

}
