package handler

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/controller_connection"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/utils"
	"google.golang.org/protobuf/proto"
)

const AggregatorPrefix = "aggregator"

type MessageHandler struct {
	// queue connections
	aggregatorQueue    middleware.MessageMiddleware
	processedDataQueue middleware.MessageMiddleware

	// internal data
	currentClientID     string
	storePath           string
	nodeConnectionQueue middleware.MessageMiddleware
	messageHandlers     map[enum.TaskType]func(enum.TaskType, []byte) error
	workerName          string
	stopConsuming       chan bool
	batchSize           int
	aggregatorService   *business.AggregatorService
}

func NewMessageHandler(address string, storePath string, batchSize int, aggregatorService *business.AggregatorService) *MessageHandler {

	workerName := fmt.Sprintf("%s_%s", AggregatorPrefix, uuid.New().String())
	mh := &MessageHandler{
		aggregatorService:   aggregatorService,
		aggregatorQueue:     middleware.GetAggregatorQueue(address),
		processedDataQueue:  middleware.GetProcessedDataQueue(address),
		nodeConnectionQueue: middleware.GetNodeConnectionsQueue(address),
		workerName:          workerName,
		storePath:           storePath,
		batchSize:           batchSize,
	}

	mh.stopConsuming = make(chan bool, 1)

	return mh
}

func (mh *MessageHandler) Close() error {

	mh.stopConsuming <- true

	e := mh.aggregatorQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing join queue middleware: %v", e)
	}

	e = mh.processedDataQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing processed data queue middleware: %v", e)
	}

	e = mh.nodeConnectionQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing controller connection middleware: %v", e)
	}

	return nil
}

func (mh *MessageHandler) AnnounceToController() error {

	log.Debug("Announcing to controller")
	e := mh.sendMessageToNodeConnection(
		mh.workerName,
		false,
	)
	if e != nil {
		return e
	}
	return nil
}

func (mh *MessageHandler) Start(
	msgHandler func(payload []byte, taskType int32) error,
) (enum.TaskType, error) {
	defer mh.aggregatorQueue.StopConsuming()

	isFirstMessage := true
	DoneTaskType := make(chan enum.TaskType, 1)

	mh.aggregatorQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		log.Debug("Started consuming messages from aggregator queue")
		for msg := range consumeChannel {
			err := msg.Ack(false)
			if err != nil {
				return
			}

			dataBatch, err := utils.GetDataBatch(msg.Body)
			if err != nil {
				log.Errorf("Failed to get data batch: %v", err)
				continue
			}

			if isFirstMessage {
				mh.currentClientID = dataBatch.GetClientId()
				isFirstMessage = false
			}

			payload := dataBatch.GetPayload()
			taskType := dataBatch.GetTaskType()
			done := dataBatch.GetDone()

			if done {
				log.Info("Received done message. Finishing processing.")
				DoneTaskType <- enum.TaskType(taskType)
				mh.stopConsuming <- true
				return
			}

			err = msgHandler(payload, taskType)
			if err != nil {
				log.Errorf("Failed to process message: %v", err)
				continue
			}

		}
	})
	<-mh.stopConsuming

	return <-DoneTaskType, nil
}

func (mh *MessageHandler) SendDone() error {

	log.Debug("Sending done message to controller")

	e := mh.sendMessageToNodeConnection(
		mh.workerName,
		true,
	)
	if e != nil {
		return e
	}

	return nil
}

func (mh *MessageHandler) SendDoneBatchToGateway(doneTaskType enum.TaskType, clientId string) error {
	batch := &data_batch.DataBatch{
		TaskType: int32(doneTaskType),
		ClientId: clientId,
		Done:     true,
	}

	serializedData, marshalErr := proto.Marshal(batch)
	if marshalErr != nil {
		return marshalErr
	}

	err := mh.sendToQueues(doneTaskType, serializedData, mh.processedDataQueue)
	if err != nil {
		return err
	}

	return nil
}

func (mh *MessageHandler) SendData(batch proto.Message, doneTaskType enum.TaskType) error {
	serializedData, marshalErr := proto.Marshal(batch)
	if marshalErr != nil {
		return marshalErr
	}

	err := mh.sendToQueues(doneTaskType, serializedData, mh.processedDataQueue)
	if err != nil {
		return err
	}

	return nil
}

func (mh *MessageHandler) sendToQueues(
	doneTaskType enum.TaskType,
	payload []byte,
	queues ...middleware.MessageMiddleware,
) error {
	dataBatch := &data_batch.DataBatch{
		TaskType: int32(doneTaskType),
		ClientId: mh.currentClientID,
		Payload:  payload,
		Done:     false,
	}
	serializedDataBatch, err := proto.Marshal(dataBatch)
	if err != nil {
		return err
	}

	for _, queue := range queues {
		if e := queue.Send(serializedDataBatch); e != middleware.MessageMiddlewareSuccess {
			return fmt.Errorf("failed to send data to queue: %d", e)
		}
	}
	return nil
}

func (mh *MessageHandler) sendMessageToNodeConnection(
	workerName string,
	isFinished bool,
) error {
	announceMsg := &controller_connection.ControllerConnection{
		WorkerName: workerName,
		ClientId:   mh.currentClientID,
		Finished:   isFinished,
	}
	msgBytes, err := proto.Marshal(announceMsg)
	if err != nil {
		return err
	}

	e := mh.nodeConnectionQueue.Send(msgBytes)
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("A error occurred while sending message to controller connection: %d", int(e))
	}

	return nil
}

func (mh *MessageHandler) SendAllData(taskType enum.TaskType) error {
	switch taskType {
	case enum.T1:
		aggregatedData, err := mh.aggregatorService.AggregateDataTask1(mh.storePath)
		if err != nil {
			return err
		}
		log.Debugf("Aggregated data for Task 1, with %d items", len(aggregatedData))

		err = mh.SendAggregateDataTask1(aggregatedData)
		if err != nil {
			return err
		}
	case enum.T2:
		topBestSelling, err := mh.aggregatorService.AggregateBestSellingData(mh.storePath)
		if err != nil {
			return err
		}

		err = mh.SendAggregateDataBestSelling(topBestSelling)
		if err != nil {
			return err
		}

		topMostProfits, errS := mh.aggregatorService.AggregateMostProfitsData(mh.storePath)
		if errS != nil {
			return err
		}

		return mh.SendAggregateDataMostProfits(topMostProfits)
	case enum.T3:
		aggregatedData, err := mh.aggregatorService.AggregateDataTask3(mh.storePath)
		if err != nil {
			return err
		}

		return mh.SendAggregateDataTask3(aggregatedData)
	case enum.T4:
		topMostPurchases, err := mh.aggregatorService.AggregateDataTask4(mh.storePath)
		if err != nil {
			return err
		}

		return mh.SendAggregateDataTask4(topMostPurchases)
	default:
		return fmt.Errorf("unknown task type: %v", taskType)
	}
	return nil
}
