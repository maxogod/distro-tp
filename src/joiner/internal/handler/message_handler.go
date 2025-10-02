package handler

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/controller_connection"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/utils"
	"google.golang.org/protobuf/proto"
)

const FilterPrefix = "filter"

type MessageHandler struct {
	// connection data
	joinerQueue     middleware.MessageMiddleware
	aggregatorQueue middleware.MessageMiddleware

	// internal data
	currentClientID     string
	nodeConnectionQueue middleware.MessageMiddleware
	workerName          string
	stopConsuming       chan bool
}

func NewMessageHandler(
	Address string,
) *MessageHandler {

	workerName := fmt.Sprintf("%s_%s", FilterPrefix, uuid.New().String())
	mh := &MessageHandler{
		joinerQueue:         middleware.GetJoinerQueue(Address),
		aggregatorQueue:     middleware.GetAggregatorQueue(Address),
		nodeConnectionQueue: middleware.GetNodeConnectionsQueue(Address),
		workerName:          workerName,
	}

	mh.stopConsuming = make(chan bool, 1)

	return mh
}

func (mh *MessageHandler) Close() error {

	mh.stopConsuming <- true

	e := mh.joinerQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing joiner queue middleware: %v", e)
	}

	e = mh.aggregatorQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing process data queue middleware: %v", e)
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
	dataCallback func(payload []byte, taskType int32) error,
	referenceCallback func(payload []byte, taskType int32) error,
) error {
	defer mh.joinerQueue.StopConsuming()

	isFirstMessage := true
	mh.joinerQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
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
			isReference := dataBatch.GetIsReferenceData()

			if done && !isReference {
				log.Info("Received done message. Finishing processing.")
				mh.stopConsuming <- true
				return
			}

			if isReference {
				err = referenceCallback(payload, taskType)
			} else {
				err = dataCallback(payload, taskType)
			}

			if err != nil {
				log.Errorf("Failed to process message: %v", err)
				continue
			}
			if err != nil {
				log.Errorf("Failed to process message: %v", err)
				continue
			}

		}
	})
	<-mh.stopConsuming

	return nil
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

func (mh *MessageHandler) SendData(taskType enum.TaskType, serializedData []byte) error {

	return mh.sendToQueues(taskType, serializedData, mh.aggregatorQueue)

}

func (mh *MessageHandler) sendToQueues(
	taskType enum.TaskType,
	payload []byte,
	queues ...middleware.MessageMiddleware,
) error {
	dataBatch := &data_batch.DataBatch{
		TaskType: int32(taskType),
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
			return fmt.Errorf("Failed to send filtered data to queue: %d", e)
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
