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

var log = logger.GetLogger()

type joinerExecutor struct {
	config          *config.Config
	joinerService   business.JoinerService
	aggregatorQueue middleware.MessageMiddleware
}

func NewJoinerExecutor(config *config.Config,
	joinerService business.JoinerService,
	aggregatorQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &joinerExecutor{
		config:          config,
		joinerService:   joinerService,
		aggregatorQueue: aggregatorQueue,
	}
}

// HandleTask2 is exclusively for reference data
func (je *joinerExecutor) HandleTask2(payload []byte, clientID string) error {
	dataEnvelope := &protocol.DataEnvelope{}
	err := proto.Unmarshal(payload, dataEnvelope)
	if err != nil {
		return err
	}

	if !dataEnvelope.GetIsRef() {
		panic("Received a non-reference data envelope for Task 2, ignoring...")
	}

	if !dataEnvelope.GetIsDone() {
		return je.handleRefData(dataEnvelope, clientID)
	}
	return je.joinerService.FinishStoringRefData(clientID)

}

// HandleTask2_1 is exclusively for reduced data
func (je *joinerExecutor) HandleTask2_1(payload []byte, clientID string) error {
	dataEnvelope := &protocol.DataEnvelope{}
	err := proto.Unmarshal(payload, dataEnvelope)
	if err != nil {
		return err
	}

	if dataEnvelope.GetIsRef() {
		panic("The joiner only implements Task 2_1 for reduced data")
	}

	reducedData := &reduced.TotalProfitBySubtotal{}
	err = proto.Unmarshal(dataEnvelope.GetPayload(), reducedData)
	if err != nil {
		return err
	}

	joinedData := je.joinerService.JoinTotalProfitBySubtotal(reducedData, clientID)
	if len(joinedData) == 0 {
		return nil
	}

	for _, jd := range joinedData {
		err = worker.SendDataToMiddleware(jd, enum.T2_1, clientID, je.aggregatorQueue)
		if err != nil {
			return err
		}
	}

	return nil
}

// HandleTask2_2 is exclusively for reduced data
func (je *joinerExecutor) HandleTask2_2(payload []byte, clientID string) error {
	dataEnvelope := &protocol.DataEnvelope{}
	err := proto.Unmarshal(payload, dataEnvelope)
	if err != nil {
		return err
	}

	if dataEnvelope.GetIsRef() {
		panic("The joiner only implements Task 2_2 for reduced data")
	}

	reducedData := &reduced.TotalSoldByQuantity{}
	err = proto.Unmarshal(dataEnvelope.GetPayload(), reducedData)
	if err != nil {
		return err
	}

	joinedData := je.joinerService.JoinTotalSoldByQuantity(reducedData, clientID)
	if len(joinedData) == 0 {
		return nil
	}

	for _, jd := range joinedData {
		err = worker.SendDataToMiddleware(jd, enum.T2_2, clientID, je.aggregatorQueue)
		if err != nil {
			return err
		}
	}

	return nil
}

func (je *joinerExecutor) HandleTask3(payload []byte, clientID string) error {
	dataEnvelope := &protocol.DataEnvelope{}
	err := proto.Unmarshal(payload, dataEnvelope)
	if err != nil {
		return err
	}

	if dataEnvelope.GetIsRef() {
		if !dataEnvelope.GetIsDone() {
			return je.handleRefData(dataEnvelope, clientID)
		}
		return je.joinerService.FinishStoringRefData(clientID)
	}

	reducedData := &reduced.TotalPaymentValue{}

	err = proto.Unmarshal(dataEnvelope.GetPayload(), reducedData)
	if err != nil {
		return err
	}

	joinedData := je.joinerService.JoinTotalPaymentValue(reducedData, clientID)
	if len(joinedData) == 0 {
		return nil
	}

	for _, jd := range joinedData {
		err = worker.SendDataToMiddleware(jd, enum.T3, clientID, je.aggregatorQueue)
		if err != nil {
			return err
		}
	}

	return nil
}

func (je *joinerExecutor) HandleTask4(payload []byte, clientID string) error {
	dataEnvelope := &protocol.DataEnvelope{}
	err := proto.Unmarshal(payload, dataEnvelope)
	if err != nil {
		return err
	}

	if dataEnvelope.GetIsRef() {
		if !dataEnvelope.GetIsDone() {
			return je.handleRefData(dataEnvelope, clientID)
		}
		return je.joinerService.FinishStoringRefData(clientID)
	}

	countedData := &reduced.CountedUserTransactions{}
	err = proto.Unmarshal(dataEnvelope.GetPayload(), countedData)
	if err != nil {
		return err
	}

	joinedData := je.joinerService.JoinCountedUserTransactions(countedData, clientID)
	if len(joinedData) == 0 {
		return nil
	}

	for _, jd := range joinedData {
		err = worker.SendDataToMiddleware(jd, enum.T4, clientID, je.aggregatorQueue)
		if err != nil {
			return err
		}
	}

	return nil
}

func (je *joinerExecutor) HandleFinishClient(clientID string) error {
	log.Debug("Finishing client: ", clientID)

	return je.joinerService.DeleteClientRefData(clientID)
}

func (je *joinerExecutor) Close() error {
	err := je.joinerService.Close()
	je.aggregatorQueue.Close()
	if err != nil {
		return err
	}
	return nil
}

func (je *joinerExecutor) HandleTask1(payload []byte, clientID string) error {
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
		log.Errorf("Unknown reference type: %v", refData.GetReferenceType())
		return nil
	}
}
