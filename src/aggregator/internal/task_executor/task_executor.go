package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker"
	"google.golang.org/protobuf/proto"
)

type AggregatorExecutor struct {
	config             TaskConfig
	aggregatorService  business.AggregatorService
	processedDataQueue middleware.MessageMiddleware
}

const TRANSACTION_SEND_LIMIT = 1000

func NewAggregatorExecutor(config TaskConfig, aggregatorService business.AggregatorService, processedDataQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &AggregatorExecutor{
		config:             config,
		aggregatorService:  aggregatorService,
		processedDataQueue: processedDataQueue,
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
	// TODO: implement later
	return nil
}

func (ae *AggregatorExecutor) HandleTask2_2(payload []byte, clientID string) error {
	return nil
}

func (ae *AggregatorExecutor) HandleTask3(payload []byte, clientID string) error {
	// TODO: implement later
	return nil
}

func (ae *AggregatorExecutor) HandleTask4(payload []byte, clientID string) error {
	// TODO: implement later
	return nil
}

func (ae *AggregatorExecutor) HandleFinishClient(clientID string) error {

	for transactions, moreBatches := ae.aggregatorService.GetStoredTransactions(clientID, TRANSACTION_SEND_LIMIT); moreBatches; transactions, moreBatches = ae.aggregatorService.GetStoredTransactions(clientID, TRANSACTION_SEND_LIMIT) {
		transactionBatch := &raw.TransactionBatch{
			Transactions: transactions,
		}

		if err := worker.SendDataToMiddleware(transactionBatch, enum.T1, clientID, ae.processedDataQueue); err != nil {
			return fmt.Errorf("failed to send data to middleware: %v", err)
		}
	}
	return worker.SendDone(clientID, ae.processedDataQueue)

}

func (ae *AggregatorExecutor) Close() error {

	e := ae.processedDataQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close processed data queue: %v", e)
	}

	return nil
}

func (ae *AggregatorExecutor) HandleTask2(payload []byte, clientID string) error {
	panic("The filter does not implement Task 2")
}
