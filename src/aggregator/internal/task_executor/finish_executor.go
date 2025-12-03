package task_executor

import (
	"fmt"
	"sync"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
)

type finishExecutor struct {
	address           string
	aggregatorService business.AggregatorService
	finishExecutors   map[enum.TaskType]func(clientID string) error
	outputQueue       middleware.MessageMiddleware
	limits            config.Limits
	ackHandlers       *sync.Map // map[clientTask][]func(bool, bool) error
}

func NewFinishExecutor(address string, aggregatorService business.AggregatorService, outputQueue middleware.MessageMiddleware, limits config.Limits, ackHandlers *sync.Map) FinishExecutor {
	fe := finishExecutor{
		address:           address,
		aggregatorService: aggregatorService,
		outputQueue:       outputQueue,
		limits:            limits,
		ackHandlers:       ackHandlers,
	}

	fe.finishExecutors = map[enum.TaskType]func(clientID string) error{
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

	if err := finishFunc(clientID); err != nil {
		return err
	}

	return fe.AckClientMessages(clientID)
}

func (fe *finishExecutor) AckClientMessages(clientID string) error {
	value, ok := fe.ackHandlers.Load(clientID)
	if !ok {
		return nil
	}
	handlers, ok := value.([]func(bool, bool) error)
	if !ok {
		return fmt.Errorf("invalid ack handlers type for client %s", clientID)
	}
	for i, handler := range handlers {
		if err := handler(true, false); err != nil {
			return fmt.Errorf("error executing ack handler %d for client %s: %v", i, clientID, err)
		}
	}
	fe.ackHandlers.Delete(clientID)

	logger.Logger.Debugf("[%s] Acked %d messages", clientID, len(handlers))

	return nil
}

// ----------------- private finish handlers ----------------

func (fe *finishExecutor) finishTask2(clientID string) error {
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
