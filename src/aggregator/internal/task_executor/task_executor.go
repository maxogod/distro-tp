package task_executor

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type AggregatorExecutor struct {
	config            *config.Config
	aggregatorService business.AggregatorService
}

// TODO: Move to config
const TRANSACTION_SEND_LIMIT = 1000

func NewAggregatorExecutor(config *config.Config, aggregatorService business.AggregatorService) worker.TaskExecutor {
	return &AggregatorExecutor{
		config:            config,
		aggregatorService: aggregatorService,
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
	// TODO: consider using batch insert in the future
	reducedDataArray := []*reduced.TotalProfitBySubtotal{reducedData}

	return ae.aggregatorService.StoreTotalProfitBySubtotal(clientID, reducedDataArray)
}

func (ae *AggregatorExecutor) HandleTask2_2(payload []byte, clientID string) error {
	reducedData := &reduced.TotalSoldByQuantity{}
	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}
	// TODO: consider using batch insert in the future
	reducedDataArray := []*reduced.TotalSoldByQuantity{reducedData}

	return ae.aggregatorService.StoreTotalSoldByQuantity(clientID, reducedDataArray)
}

func (ae *AggregatorExecutor) HandleTask3(payload []byte, clientID string) error {
	reducedData := &reduced.TotalPaymentValue{}
	err := proto.Unmarshal(payload, reducedData)
	if err != nil {
		return err
	}
	// TODO: consider using batch insert in the future
	reducedDataArray := []*reduced.TotalPaymentValue{reducedData}

	return ae.aggregatorService.StoreTotalPaymentValue(clientID, reducedDataArray)
}

func (ae *AggregatorExecutor) HandleTask4(payload []byte, clientID string) error {
	countedData := &reduced.CountedUserTransactions{}
	err := proto.Unmarshal(payload, countedData)
	if err != nil {
		return err
	}
	// TODO: consider using batch insert in the future
	countedDataArray := []*reduced.CountedUserTransactions{countedData}

	return ae.aggregatorService.StoreCountedUserTransactions(clientID, countedDataArray)
}

func (ae *AggregatorExecutor) HandleFinishClient(clientID string) error {

	processedDataQueue := middleware.GetProcessedDataExchange(ae.config.Address, clientID)

	defer processedDataQueue.Close()

	log.Debug("Finishing client: ", clientID)
	// ==================
	err := ae.sendAllTotalPaymentValue(clientID, processedDataQueue)
	if err != nil {
		return fmt.Errorf("failed to send all total payment values: %v", err)
	}
	// ==================

	err = worker.SendDone(clientID, processedDataQueue)
	if err != nil {
		return fmt.Errorf("failed to send done message to middleware: %v", err)
	}

	log.Debug("Client Finished: ", clientID)

	return nil
}

func (ae *AggregatorExecutor) sendAllTransactions(clientID string, processedDataQueue middleware.MessageMiddleware) error {
	for {
		transactions, moreBatches := ae.aggregatorService.GetStoredTransactions(clientID, TRANSACTION_SEND_LIMIT)
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
	return nil
}

func (ae *AggregatorExecutor) sendAllTotalPaymentValue(clientID string, processedDataQueue middleware.MessageMiddleware) error {
	for {
		totalPaymentValueBatch, moreBatches := ae.aggregatorService.GetStoredTotalPaymentValue(clientID, TRANSACTION_SEND_LIMIT)
		if !moreBatches {
			break
		}

		for _, totalPaymentValue := range totalPaymentValueBatch {
			if err := worker.SendDataToMiddleware(totalPaymentValue, enum.T1, clientID, processedDataQueue); err != nil {
				return fmt.Errorf("failed to send data to middleware: %v", err)
			}
		}
	}
	return nil
}

func (ae *AggregatorExecutor) Close() error {
	return ae.aggregatorService.Close()
}

func (ae *AggregatorExecutor) HandleTask2(payload []byte, clientID string) error {
	panic("The filter does not implement Task 2")
}
