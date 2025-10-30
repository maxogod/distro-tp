package task_executor

import (
	"fmt"

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
	config           *config.Config
	connectedClients map[string]middleware.MessageMiddleware
	joinerService    business.JoinerService
	aggregatorQueue  middleware.MessageMiddleware
	joinerQueue      middleware.MessageMiddleware
}

func NewJoinerExecutor(config *config.Config,
	connectedClients map[string]middleware.MessageMiddleware,
	joinerService business.JoinerService,
	aggregatorQueue middleware.MessageMiddleware) worker.TaskExecutor {
	return &joinerExecutor{
		config:           config,
		connectedClients: connectedClients,
		joinerService:    joinerService,
		aggregatorQueue:  aggregatorQueue,
		joinerQueue:      middleware.GetJoinerQueue(config.Address), // TODO: MOVE THIS OUT LATER!!
	}
}

// HandleTask2 is exclusively for reference data
func (je *joinerExecutor) HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	defer ackHandler(shouldAck, false)

	clientID := dataEnvelope.GetClientId()

	if !dataEnvelope.GetIsRef() {
		panic("Received a non-reference data envelope for Task 2, ignoring...")
	}

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

// HandleTask2_1 is exclusively for reduced data
func (je *joinerExecutor) HandleTask2_1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	clientID := dataEnvelope.GetClientId()

	if dataEnvelope.GetIsRef() {
		panic("The joiner only implements Task 2_1 for reduced data")
	}

	reducedData := &reduced.TotalProfitBySubtotal{}
	err := proto.Unmarshal(dataEnvelope.GetPayload(), reducedData)
	if err != nil {
		return err
	}

	joinedData := je.joinerService.JoinTotalProfitBySubtotal(reducedData, clientID)

	amountSent := 0
	for _, jd := range joinedData {
		err = worker.SendDataToMiddleware(jd, enum.T2_1, clientID, je.aggregatorQueue)
		if err != nil {
			shouldRequeue = true
			return err
		}
		amountSent++
	}
	shouldAck = true

	_, exists := je.connectedClients[clientID]
	if !exists {
		je.connectedClients[clientID] = middleware.GetCounterExchange(je.config.Address, clientID+"@"+string(enum.JoinerWorker))
	}
	counterExchange := je.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, amountSent, enum.JoinerWorker, enum.AggregatorWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

// HandleTask2_2 is exclusively for reduced data
func (je *joinerExecutor) HandleTask2_2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

	clientID := dataEnvelope.GetClientId()

	if dataEnvelope.GetIsRef() {
		panic("The joiner only implements Task 2_2 for reduced data")
	}

	reducedData := &reduced.TotalSoldByQuantity{}
	err := proto.Unmarshal(dataEnvelope.GetPayload(), reducedData)
	if err != nil {
		return err
	}

	joinedData := je.joinerService.JoinTotalSoldByQuantity(reducedData, clientID)

	amountSent := 0
	for _, jd := range joinedData {
		err = worker.SendDataToMiddleware(jd, enum.T2_2, clientID, je.aggregatorQueue)
		if err != nil {
			shouldRequeue = true
			return err
		}
		amountSent++
	}
	shouldAck = true

	_, exists := je.connectedClients[clientID]
	if !exists {
		je.connectedClients[clientID] = middleware.GetCounterExchange(je.config.Address, clientID+"@"+string(enum.JoinerWorker))
	}
	counterExchange := je.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, amountSent, enum.JoinerWorker, enum.AggregatorWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (je *joinerExecutor) HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)

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

	reducedData := &reduced.TotalPaymentValueBatch{}
	err := proto.Unmarshal(dataEnvelope.GetPayload(), reducedData)
	if err != nil {
		return err
	}

	for i, rData := range reducedData.GetTotalPaymentValues() {
		joinedData, err := je.joinerService.JoinTotalPaymentValue(rData, clientID)
		if err != nil {
			// if the ref data is not present yet, requeue the message
			payload, _ := proto.Marshal(dataEnvelope)
			je.joinerQueue.Send(payload)
			shouldAck = true
			return nil
		}
		reducedData.TotalPaymentValues[i] = joinedData
	}

	err = worker.SendDataToMiddleware(reducedData, enum.T3, clientID, je.aggregatorQueue)
	if err != nil {
		shouldRequeue = true
		return err
	}
	shouldAck = true

	_, exists := je.connectedClients[clientID]
	if !exists {
		je.connectedClients[clientID] = middleware.GetCounterExchange(je.config.Address, clientID+"@"+string(enum.JoinerWorker))
	}
	counterExchange := je.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 1, enum.JoinerWorker, enum.AggregatorWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (je *joinerExecutor) HandleTask4(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	shouldRequeue := false
	defer ackHandler(shouldAck, shouldRequeue)
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

	countedDataBatch := &reduced.CountedUserTransactionBatch{}
	err := proto.Unmarshal(dataEnvelope.GetPayload(), countedDataBatch)
	if err != nil {
		return err
	}
	for i, countedData := range countedDataBatch.GetCountedUserTransactions() {
		joinedData, err := je.joinerService.JoinCountedUserTransactions(countedData, clientID)
		if err != nil {
			// if the ref data is not present yet, requeue the message
			payload, _ := proto.Marshal(dataEnvelope)
			je.joinerQueue.Send(payload)
			shouldAck = true
			return nil
		}
		countedDataBatch.CountedUserTransactions[i] = joinedData
	}

	err = worker.SendDataToMiddleware(countedDataBatch, enum.T4, clientID, je.aggregatorQueue)
	if err != nil {
		shouldRequeue = true
		log.Debugf("An error occurred: %s", err)
		return err
	}
	shouldAck = true

	_, exists := je.connectedClients[clientID]
	if !exists {
		je.connectedClients[clientID] = middleware.GetCounterExchange(je.config.Address, clientID+"@"+string(enum.JoinerWorker))
	}
	counterExchange := je.connectedClients[clientID]
	if err := worker.SendCounterMessage(clientID, 1, enum.JoinerWorker, enum.AggregatorWorker, counterExchange); err != nil {
		return err
	}

	return nil
}

func (je *joinerExecutor) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	shouldAck := false
	defer ackHandler(shouldAck, false)

	clientID := dataEnvelope.GetClientId()
	log.Debug("Finishing client: ", clientID)

	err := je.joinerService.DeleteClientRefData(clientID)
	if err != nil {
		return err
	}
	shouldAck = true

	return nil
}

func (je *joinerExecutor) Close() error {
	if err := je.joinerService.Close(); err != nil {
		return err
	}

	if err := je.aggregatorQueue.Close(); err != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close aggregator queue: %v", err)
	}

	for clientID, exchange := range je.connectedClients {
		if e := exchange.Close(); e != middleware.MessageMiddlewareSuccess {
			return fmt.Errorf("failed to close counter exchange for client %s: %v", clientID, e)
		}
	}

	return nil
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
		log.Errorf("Unknown reference type: %v", refData.GetReferenceType())
		return nil
	}
}
