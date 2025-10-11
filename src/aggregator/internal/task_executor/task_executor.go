package task_executor

import (
	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type AggregatorExecutor struct {
	config            *config.Config
	aggregatorService business.AggregatorService
	finishExecutor    FinishExecutor
}

// TODO: Move to config
const TRANSACTION_SEND_LIMIT = 1000

func NewAggregatorExecutor(config *config.Config, aggregatorService business.AggregatorService) worker.TaskExecutor {
	return &AggregatorExecutor{
		config:            config,
		aggregatorService: aggregatorService,
		finishExecutor:    NewFinishExecutor(aggregatorService),
	}
}

func (ae *AggregatorExecutor) HandleTask1(payload []byte, clientID string) error {

	transactionBatch := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactionBatch)
	if err != nil {
		return err
	}

	return ae.aggregatorService.StoreTransactions(clientID, transactionBatch.Transactions)
}

func (ae *AggregatorExecutor) HandleTask2_1(payload []byte, clientID string) error {
	reducedData := &reduced.TotalProfitBySubtotal{}
	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	return ae.aggregatorService.StoreTotalProfitBySubtotal(clientID, reducedData)
}

func (ae *AggregatorExecutor) HandleTask2_2(payload []byte, clientID string) error {
	reducedData := &reduced.TotalSoldByQuantity{}
	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	return ae.aggregatorService.StoreTotalSoldByQuantity(clientID, reducedData)
}

func (ae *AggregatorExecutor) HandleTask3(payload []byte, clientID string) error {
	reducedData := &reduced.TotalPaymentValue{}
	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}

	return ae.aggregatorService.StoreTotalPaymentValue(clientID, reducedData)
}

func (ae *AggregatorExecutor) HandleTask4(payload []byte, clientID string) error {
	countedData := &reduced.CountedUserTransactions{}
	err := proto.Unmarshal(payload, countedData)
	if err != nil {
		return err
	}

	return ae.aggregatorService.StoreCountedUserTransactions(clientID, countedData)
}

func (ae *AggregatorExecutor) HandleFinishClient(clientID string) error {

	processedDataQueue := middleware.GetProcessedDataExchange(ae.config.Address, clientID)

	defer processedDataQueue.Close()

	log.Debug("Finishing client: ", clientID)
	// ==================
	// use the finish exectutor to sort and send all data, 
	// depending on the task type and client ID
	// ==================

	log.Debug("Client Finished: ", clientID)

	return nil
}

func (ae *AggregatorExecutor) Close() error {
	return ae.aggregatorService.Close()
}

func (ae *AggregatorExecutor) HandleTask2(payload []byte, clientID string) error {
	panic("The filter does not implement Task 2")
}
