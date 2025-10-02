package handler

import (
	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

func (mh *MessageHandler) SendAggregateDataTask1(items business.MapTransactions) error {
	var currentBatch []*raw.Transaction
	for _, item := range items {
		currentBatch = append(currentBatch, item)
		if len(currentBatch) >= mh.batchSize {
			batch := &raw.TransactionBatch{Transactions: currentBatch}
			if sendErr := mh.SendData(batch, enum.T1); sendErr != nil {
				return sendErr
			}
			log.Debugf("Sent batch of %d transactions", len(currentBatch))
			currentBatch = []*raw.Transaction{}
		}
	}

	if len(currentBatch) > 0 {
		batch := &raw.TransactionBatch{Transactions: currentBatch}
		if sendErr := mh.SendData(batch, enum.T1); sendErr != nil {
			return sendErr
		}
		log.Debugf("Sent final batch of %d transactions", len(currentBatch))
	}

	if err := mh.SendDoneBatchToGateway(enum.T1, mh.currentClientID); err != nil {
		return err
	}

	return nil
}

func (mh *MessageHandler) SendAggregateDataTask4(items business.MapJoinMostPurchasesUser) error {
	var currentBatch []*joined.JoinMostPurchasesUser
	for _, item := range items {
		currentBatch = append(currentBatch, item)
		if len(currentBatch) >= mh.batchSize {
			batch := &joined.JoinMostPurchasesUserBatch{Users: currentBatch}
			if sendErr := mh.SendData(batch, enum.T1); sendErr != nil {
				return sendErr
			}
			currentBatch = []*joined.JoinMostPurchasesUser{}
		}
	}

	if len(currentBatch) > 0 {
		batch := &joined.JoinMostPurchasesUserBatch{Users: currentBatch}
		if sendErr := mh.SendData(batch, enum.T1); sendErr != nil {
			return sendErr
		}
	}

	if err := mh.SendDoneBatchToGateway(enum.T1, mh.currentClientID); err != nil {
		return err
	}

	return nil
}

func (mh *MessageHandler) SendAggregateDataTask3(items business.MapJoinStoreTPV) error {
	var currentBatch []*joined.JoinStoreTPV
	for _, item := range items {
		currentBatch = append(currentBatch, item)
		if len(currentBatch) >= mh.batchSize {
			batch := &joined.JoinStoreTPVBatch{Items: currentBatch}
			if sendErr := mh.SendData(batch, enum.T1); sendErr != nil {
				return sendErr
			}
			currentBatch = []*joined.JoinStoreTPV{}
		}
	}

	if len(currentBatch) > 0 {
		batch := &joined.JoinStoreTPVBatch{Items: currentBatch}
		if sendErr := mh.SendData(batch, enum.T1); sendErr != nil {
			return sendErr
		}
	}

	if err := mh.SendDoneBatchToGateway(enum.T1, mh.currentClientID); err != nil {
		return err
	}

	return nil
}

func (mh *MessageHandler) SendAggregateDataBestSelling(items business.MapJoinBestSelling) error {
	var currentBatch []*joined.JoinBestSellingProducts
	for _, item := range items {
		currentBatch = append(currentBatch, item)
		if len(currentBatch) >= mh.batchSize {
			batch := &joined.JoinBestSellingProductsBatch{Items: currentBatch}
			if sendErr := mh.SendData(batch, enum.T1); sendErr != nil {
				return sendErr
			}
			currentBatch = []*joined.JoinBestSellingProducts{}
		}
	}

	if len(currentBatch) > 0 {
		batch := &joined.JoinBestSellingProductsBatch{Items: currentBatch}
		if sendErr := mh.SendData(batch, enum.T1); sendErr != nil {
			return sendErr
		}
	}

	if err := mh.SendDoneBatchToGateway(enum.T1, mh.currentClientID); err != nil {
		return err
	}

	return nil
}

func (mh *MessageHandler) SendAggregateDataMostProfits(items business.MapJoinMostProfits) error {
	var currentBatch []*joined.JoinMostProfitsProducts
	for _, item := range items {
		currentBatch = append(currentBatch, item)
		if len(currentBatch) >= mh.batchSize {
			batch := &joined.JoinMostProfitsProductsBatch{Items: currentBatch}
			if sendErr := mh.SendData(batch, enum.T1); sendErr != nil {
				return sendErr
			}
			currentBatch = []*joined.JoinMostProfitsProducts{}
		}
	}

	if len(currentBatch) > 0 {
		batch := &joined.JoinMostProfitsProductsBatch{Items: currentBatch}
		if sendErr := mh.SendData(batch, enum.T1); sendErr != nil {
			return sendErr
		}
	}

	if err := mh.SendDoneBatchToGateway(enum.T1, mh.currentClientID); err != nil {
		return err
	}

	return nil
}
