package task_executor

import (
	"fmt"
	"sort"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/common/worker"
	"google.golang.org/protobuf/proto"
)

// TODO: Move to config
const TRANSACTION_SEND_LIMIT = 1000
const TOP_N = 3

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
		enum.T3: fe.sortTask3,
		enum.T4: fe.sortTask4,
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

func (fe *finishExecutor) sortTask3(clientID string) error {
	sortFn := func(a, b *proto.Message) bool {
		txA := (*a).(*reduced.TotalPaymentValue)
		txB := (*b).(*reduced.TotalPaymentValue)

		yearA, halfA := utils.ParseSemester(txA.GetSemester())
		yearB, halfB := utils.ParseSemester(txB.GetSemester())

		// Compare by year first, then by half-year
		if yearA != yearB {
			// Older years come first
			return yearA < yearB
		}
		// H1 comes before H2
		return halfA < halfB
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
	bestMonthMap := make(map[string]*reduced.TotalProfitBySubtotal)
	defer processedDataQueue.Close()
	clientWithPrefix := T2_1_PREFIX + clientID
	for {
		tpsDataBatch, moreBatches := fe.aggregatorService.GetStoredTotalProfitBySubtotal(clientWithPrefix, TRANSACTION_SEND_LIMIT)
		if !moreBatches {
			break
		}
		for _, tpsData := range tpsDataBatch {
			yearMonth := tpsData.GetYearMonth()

			if existing, exists := bestMonthMap[yearMonth]; !exists || tpsData.GetSubtotal() > existing.GetSubtotal() {
				bestMonthMap[yearMonth] = tpsData
			}
		}
	}
	// Sort the keys (yearMonth) in ascending order
	sortedKeys := make([]string, 0, len(bestMonthMap))

	for yearMonth := range bestMonthMap {
		sortedKeys = append(sortedKeys, yearMonth)
	}
	sort.Strings(sortedKeys)

	// Send the data in sorted order
	for _, yearMonth := range sortedKeys {
		bestMonth := bestMonthMap[yearMonth]
		if err := worker.SendDataToMiddleware(bestMonth, enum.T2_1, clientID, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
	}
	return worker.SendDone(clientID, processedDataQueue)
}

func (fe *finishExecutor) finishTask2_2(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	bestMonthMap := make(map[string]*reduced.TotalSoldByQuantity)
	defer processedDataQueue.Close()
	clientWithPrefix := T2_2_PREFIX + clientID
	for {
		tpqDataBatch, moreBatches := fe.aggregatorService.GetStoredTotalSoldByQuantity(clientWithPrefix, TRANSACTION_SEND_LIMIT)
		if !moreBatches {
			break
		}
		for _, tpsData := range tpqDataBatch {
			yearMonth := tpsData.GetYearMonth()

			if existing, exists := bestMonthMap[yearMonth]; !exists || tpsData.GetQuantity() > existing.GetQuantity() {
				bestMonthMap[yearMonth] = tpsData
			}
		}
	}
	// Sort the keys (yearMonth) in ascending order
	sortedKeys := make([]string, 0, len(bestMonthMap))
	for yearMonth := range bestMonthMap {
		sortedKeys = append(sortedKeys, yearMonth)
	}
	sort.Strings(sortedKeys)

	// Send the data in sorted order
	for _, yearMonth := range sortedKeys {
		bestMonth := bestMonthMap[yearMonth]
		if err := worker.SendDataToMiddleware(bestMonth, enum.T2_2, clientID, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
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
			if err := worker.SendDataToMiddleware(tpvData, enum.T3, clientID, processedDataQueue); err != nil {
				return fmt.Errorf("failed to send data to middleware: %v", err)
			}
		}
	}
	return worker.SendDone(clientID, processedDataQueue)
}

func (fe *finishExecutor) finishTask4(clientID string) error {
	processedDataQueue := middleware.GetProcessedDataExchange(fe.address, clientID)
	defer processedDataQueue.Close()
	countedTransactions, _ := fe.aggregatorService.GetStoredCountedUserTransactions(clientID, TOP_N)
	for _, countedData := range countedTransactions {
		if err := worker.SendDataToMiddleware(countedData, enum.T4, clientID, processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
	}
	return worker.SendDone(clientID, processedDataQueue)
}
