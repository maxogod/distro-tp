package task_executor

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"google.golang.org/protobuf/proto"
)

const SEND_LIMIT = 1000

type joinerExecutor struct {
	config        *config.Config
	joinerService business.JoinerService
	joinerQueue   middleware.MessageMiddleware
}

func NewJoinerExecutor(config *config.Config,
	joinerService business.JoinerService,
) worker.TaskExecutor {
	return &joinerExecutor{
		config:        config,
		joinerService: joinerService,
		joinerQueue:   middleware.GetJoinerQueue(config.Address),
	}
}

// HandleTask2_2 is exclusively for reduced data
func (je *joinerExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	clientID := dataEnvelope.GetClientId()

	if dataEnvelope.GetIsRef() {
		var err error
		if !dataEnvelope.GetIsDone() {
			err = je.handleRefData(dataEnvelope, clientID)
		} else {
			err = je.joinerService.FinishStoringRefData(clientID)
		}

		if err != nil {
			return err
		}
		shouldAck = true
		return nil
	}

	processedDataQueue := middleware.GetProcessedDataExchange(je.config.Address, clientID)
	defer func() {
		ackHandler(shouldAck, shouldRequeue)
		// TODO: Now that the aggregator can die, if the joiner gets a dupped client ID,
		// it shouldn't delete it until it's really done
		if !dataEnvelope.GetIsRef() {
			processedDataQueue.Close()
			je.joinerService.DeleteClientRefData(clientID)
			logger.Logger.Debugf("Finished & Deleted ref data for client %s", clientID)
		}
	}()

	reportData := &reduced.TotalSumItemsReport{}
	err := proto.Unmarshal(dataEnvelope.GetPayload(), reportData)
	if err != nil {
		return err
	}

	// here we join the data
	for _, itemData := range reportData.GetTotalSumItemsBySubtotal() {
		if err := je.joinerService.JoinTotalSumItem(itemData, clientID); err != nil {
			// if the ref data is not present yet, requeue the message
			payload, _ := proto.Marshal(dataEnvelope)
			je.joinerQueue.Send(payload)
			shouldAck = true
			return nil
		}
	}
	for _, itemData := range reportData.GetTotalSumItemsByQuantity() {
		if err := je.joinerService.JoinTotalSumItem(itemData, clientID); err != nil {
			// in the first for-loop we already checked for missing ref data, so if we error here it's another issue
			return err
		}
	}
	err = worker.SendDataToMiddleware(reportData, enum.T2, clientID, 0, processedDataQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}

	shouldAck = true
	return nil
}

func (je *joinerExecutor) HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	clientID := dataEnvelope.GetClientId()

	if dataEnvelope.GetIsRef() {
		var err error
		if !dataEnvelope.GetIsDone() {
			err = je.handleRefData(dataEnvelope, clientID)
		} else {
			err = je.joinerService.FinishStoringRefData(clientID)
		}

		if err != nil {
			return err
		}
		shouldAck = true
		return nil
	}

	processedDataQueue := middleware.GetProcessedDataExchange(je.config.Address, clientID)
	defer func() {
		ackHandler(shouldAck, shouldRequeue)
		if !dataEnvelope.GetIsRef() {
			processedDataQueue.Close()
			je.joinerService.DeleteClientRefData(clientID)
			logger.Logger.Debugf("Finished & Deleted ref data for client %s", clientID)
		}
	}()

	reducedData := &reduced.TotalPaymentValueBatch{}
	err := proto.Unmarshal(dataEnvelope.GetPayload(), reducedData)
	if err != nil {
		return err
	}

	for _, rData := range reducedData.GetTotalPaymentValues() {
		err := je.joinerService.JoinTotalPaymentValue(rData, clientID)
		if err != nil {
			// if the ref data is not present yet, requeue the message
			payload, _ := proto.Marshal(dataEnvelope)
			je.joinerQueue.Send(payload)
			shouldAck = true
			return nil
		}
	}

	err = worker.SendDataToMiddleware(reducedData, enum.T3, clientID, 0, processedDataQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true
	return nil
}

func (je *joinerExecutor) HandleTask4(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	clientID := dataEnvelope.GetClientId()

	if dataEnvelope.GetIsRef() {
		var err error
		if !dataEnvelope.GetIsDone() {
			err = je.handleRefData(dataEnvelope, clientID)
		} else {
			err = je.joinerService.FinishStoringRefData(clientID)
		}

		if err != nil {
			return err
		}
		shouldAck = true
		return nil
	}

	processedDataQueue := middleware.GetProcessedDataExchange(je.config.Address, clientID)
	defer func() {
		ackHandler(shouldAck, shouldRequeue)
		if !dataEnvelope.GetIsRef() {
			processedDataQueue.Close()
			je.joinerService.DeleteClientRefData(clientID)
			logger.Logger.Debugf("Finished & Deleted ref data for client %s", clientID)
		}
	}()

	countedDataBatch := &reduced.CountedUserTransactionBatch{}
	err := proto.Unmarshal(dataEnvelope.GetPayload(), countedDataBatch)
	if err != nil {
		return err
	}
	for _, countedData := range countedDataBatch.GetCountedUserTransactions() {
		err := je.joinerService.JoinCountedUserTransactions(countedData, clientID)
		if err != nil {
			// if the ref data is not present yet, requeue the message
			payload, _ := proto.Marshal(dataEnvelope)
			je.joinerQueue.Send(payload)
			shouldAck = true
			return nil
		}
	}

	err = worker.SendDataToMiddleware(countedDataBatch, enum.T4, clientID, 0, processedDataQueue)
	if err != nil {
		shouldRequeue = true
		logger.Logger.Debugf("An error occurred: %s", err)
		return err
	}
	shouldAck = true
	return nil
}

func (je *joinerExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	ackHandler(true, false)
	return nil
}

func (je *joinerExecutor) Close() error {
	return je.joinerService.Close()
}

func (je *joinerExecutor) HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	panic("The joiner does not implement Task 1")
}

/* --- PRIVATE UTIL METHODS --- */

func (je *joinerExecutor) handleRefData(batch *protocol.DataEnvelope, clientID string) error {
	refData := &protocol.ReferenceEnvelope{}
	err := proto.Unmarshal(batch.GetPayload(), refData)
	if err != nil {
		return err
	}

	switch enum.ReferenceType(refData.GetReferenceType()) {
	case enum.MenuItems:
		menuItemBatch := &raw.MenuItemsBatch{}
		err := proto.Unmarshal(refData.GetPayload(), menuItemBatch)
		if err != nil {
			return err
		}
		return je.joinerService.StoreMenuItems(clientID, menuItemBatch.MenuItems)
	case enum.Users:
		userBatch := &raw.UserBatch{}
		err := proto.Unmarshal(refData.GetPayload(), userBatch)
		if err != nil {
			return err
		}
		return je.joinerService.StoreUsers(clientID, userBatch.Users)
	case enum.Stores:
		storeBatch := &raw.StoreBatch{}
		err := proto.Unmarshal(refData.GetPayload(), storeBatch)
		if err != nil {
			return err
		}
		return je.joinerService.StoreShops(clientID, storeBatch.Stores)
	default:
		logger.Logger.Errorf("Unknown reference type: %v", refData.GetReferenceType())
		return nil
	}
}
