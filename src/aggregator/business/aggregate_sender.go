package service

import (
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"google.golang.org/protobuf/proto"
)

func sendAggregateData[T proto.Message, B proto.Message](
	gatewayQueue middleware.MessageMiddleware,
	items map[string]T,
	taskType enum.TaskType,
	createSpecificBatch func([]T) B,
	batchSize int,
	clientID string,
) error {
	var currentBatch []T
	for _, item := range items {
		currentBatch = append(currentBatch, item)
		if len(currentBatch) >= batchSize {
			batch := createSpecificBatch(currentBatch)
			if sendErr := SendBatchToGateway(batch, gatewayQueue, taskType); sendErr != nil {
				return sendErr
			}
			currentBatch = []T{}
		}
	}

	if len(currentBatch) > 0 {
		batch := createSpecificBatch(currentBatch)
		if sendErr := SendBatchToGateway(batch, gatewayQueue, taskType); sendErr != nil {
			return sendErr
		}
	}

	if err := SendDoneBatchToGateway(gatewayQueue, taskType, clientID); err != nil {
		return err
	}

	return nil
}

func (a *Aggregator) SendAggregateDataTask1(items MapTransactions) error {
	return sendAggregateData(
		a.gatewayDataQueue,
		items,
		enum.T3,
		func(batch []*raw.Transaction) *raw.TransactionBatch {
			return &raw.TransactionBatch{Transactions: batch}
		},
		a.config.BatchSize,
		a.currentClientID,
	)
}

func (a *Aggregator) SendAggregateDataTask4(items MapJoinMostPurchasesUser) error {
	return sendAggregateData(
		a.gatewayDataQueue,
		items,
		enum.T4,
		func(batch []*joined.JoinMostPurchasesUser) *joined.JoinMostPurchasesUserBatch {
			return &joined.JoinMostPurchasesUserBatch{Users: batch}
		},
		a.config.BatchSize,
		a.currentClientID,
	)
}

func (a *Aggregator) SendAggregateDataTask3(items MapJoinStoreTPV) error {
	return sendAggregateData(
		a.gatewayDataQueue,
		items,
		enum.T3,
		func(batch []*joined.JoinStoreTPV) *joined.JoinStoreTPVBatch {
			return &joined.JoinStoreTPVBatch{Items: batch}
		},
		a.config.BatchSize,
		a.currentClientID,
	)
}

func (a *Aggregator) SendAggregateDataBestSelling(items MapJoinBestSelling) error {
	return sendAggregateData(
		a.gatewayDataQueue,
		items,
		enum.T2,
		func(batch []*joined.JoinBestSellingProducts) *joined.JoinBestSellingProductsBatch {
			return &joined.JoinBestSellingProductsBatch{Items: batch}
		},
		a.config.BatchSize,
		a.currentClientID,
	)
}

func (a *Aggregator) SendAggregateDataMostProfits(items MapJoinMostProfits) error {
	return sendAggregateData(
		a.gatewayDataQueue,
		items,
		enum.T2,
		func(batch []*joined.JoinMostProfitsProducts) *joined.JoinMostProfitsProductsBatch {
			return &joined.JoinMostProfitsProductsBatch{Items: batch}
		},
		a.config.BatchSize,
		a.currentClientID,
	)
}
