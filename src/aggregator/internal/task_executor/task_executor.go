package task_executor

import (
	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

// To differentiate between Task 2.1 and Task 2.2 results in the DB
const T2_1_PREFIX = "T2_1_"
const T2_2_PREFIX = "T2_2_"

type AggregatorExecutor struct {
	config            *config.Config
	aggregatorService business.AggregatorService
	finishExecutor    FinishExecutor
	clientTasks       map[string]enum.TaskType
}

// TODO: Move to config
const TRANSACTION_SEND_LIMIT = 1000

func NewAggregatorExecutor(config *config.Config, aggregatorService business.AggregatorService) worker.TaskExecutor {
	return &AggregatorExecutor{
		config:            config,
		aggregatorService: aggregatorService,
		finishExecutor:    NewFinishExecutor(config.Address, aggregatorService),
		clientTasks:       make(map[string]enum.TaskType),
	}
}

func (ae *AggregatorExecutor) HandleTask1(payload []byte, clientID string) error {

	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	ae.clientTasks[clientID] = enum.T1

	return ae.aggregatorService.StoreTransactions(clientID, transactionBatch.Transactions)
}

func (ae *AggregatorExecutor) HandleTask2_1(payload []byte, clientID string) error {
	reducedData := &reduced.TotalProfitBySubtotal{}
	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	ae.clientTasks[clientID] = enum.T2

	// To differentiate between Task 2.1 and Task 2.2 results in the DB
	clientID = T2_1_PREFIX + clientID

	return ae.aggregatorService.StoreTotalProfitBySubtotal(clientID, reducedData)
}

func (ae *AggregatorExecutor) HandleTask2_2(payload []byte, clientID string) error {
	reducedData := &reduced.TotalSoldByQuantity{}
	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	ae.clientTasks[clientID] = enum.T2

	// To differentiate between Task 2.1 and Task 2.2 results in the DB
	clientID = T2_2_PREFIX + clientID

	return ae.aggregatorService.StoreTotalSoldByQuantity(clientID, reducedData)
}

func (ae *AggregatorExecutor) HandleTask3(payload []byte, clientID string) error {
	reducedData := &reduced.TotalPaymentValue{}
	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	ae.clientTasks[clientID] = enum.T3

	return ae.aggregatorService.StoreTotalPaymentValue(clientID, reducedData)
}

func (ae *AggregatorExecutor) HandleTask4(payload []byte, clientID string) error {
	countedData := &reduced.CountedUserTransactions{}
	err := proto.Unmarshal(payload, countedData)
	if err != nil {
		return err
	}

	ae.clientTasks[clientID] = enum.T4

	return ae.aggregatorService.StoreCountedUserTransactions(clientID, countedData)
}

func (ae *AggregatorExecutor) HandleFinishClient(clientID string) error {
	log.Debug("Finishing client: ", clientID)
	taskType, exists := ae.clientTasks[clientID]
	if !exists {
		log.Warn("Client ID never sent any data: ", clientID)
		return nil
	}
	task := enum.TaskType(taskType)
	if task != enum.T1 {
		ae.finishExecutor.SortTaskData(clientID, enum.TaskType(taskType))
	}
	if task == enum.T2 {
		clientID2_1 := T2_1_PREFIX + clientID
		clientID2_2 := T2_2_PREFIX + clientID
		ae.finishExecutor.SortTaskData(clientID2_1, enum.T2_1)
		ae.finishExecutor.SortTaskData(clientID2_2, enum.T2_2)
		ae.finishExecutor.SendAllData(clientID2_1, enum.T2_1)
		ae.finishExecutor.SendAllData(clientID2_2, enum.T2_2)
		log.Debug("Client Finished: ", clientID)
		delete(ae.clientTasks, clientID)
		return nil
	}
	ae.finishExecutor.SendAllData(clientID, enum.TaskType(taskType))
	log.Debug("Client Finished: ", clientID)
	delete(ae.clientTasks, clientID)
	return nil
}

func (ae *AggregatorExecutor) Close() error {
	return ae.aggregatorService.Close()
}

func (ae *AggregatorExecutor) HandleTask2(payload []byte, clientID string) error {
	panic("The aggregator does not implement Task 2")
}
