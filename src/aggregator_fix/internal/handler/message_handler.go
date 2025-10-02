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

const AggregatorPrefix = "aggregator"

type MessageHandler struct {
	// queue connections
	joinQueue          middleware.MessageMiddleware
	processedDataQueue middleware.MessageMiddleware

	// internal data
	currentClientID     string
	nodeConnectionQueue middleware.MessageMiddleware
	messageHandlers     map[enum.TaskType]func(enum.TaskType, []byte) error
	workerName          string
	stopConsuming       chan bool
}

func NewMessageHandler(
	Address string,
) *MessageHandler {

	workerName := fmt.Sprintf("%s_%s", AggregatorPrefix, uuid.New().String())
	mh := &MessageHandler{
		joinQueue:           middleware.GetJoinerQueue(Address),
		processedDataQueue:  middleware.GetProcessedDataQueue(Address),
		nodeConnectionQueue: middleware.GetNodeConnectionsQueue(Address),
		workerName:          workerName,
	}

	mh.stopConsuming = make(chan bool, 1)

	return mh
}

func (mh *MessageHandler) Close() error {

	mh.stopConsuming <- true

	e := mh.joinQueue.Close()
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
	callback func(payload []byte, taskType int32) error,
) (enum.TaskType, error) {
	defer mh.joinQueue.StopConsuming()

	isFirstMessage := true
	DoneTaskType := make(chan enum.TaskType, 1)

	mh.joinQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
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

			if done {
				log.Info("Received done message. Finishing processing.")
				DoneTaskType <- enum.TaskType(taskType)
				mh.stopConsuming <- true
				return
			}

			err = callback(payload, taskType)
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

func (mh *MessageHandler) SendData(doneTaskType enum.TaskType) error {

	// TODO GET THE PAYLOAD FROM THE TASK SERVICE WITH CORRECT TYPE

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
