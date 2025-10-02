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

const GroupByPrefix = "group_by"

type MessageHandler struct {
	currentClientID string
	// TODO >> READ FROM ONE QUEUE NOT 2
	reduceSumQueue   middleware.MessageMiddleware
	reduceCountQueue middleware.MessageMiddleware

	// Added for joiner
	joinerQueue middleware.MessageMiddleware

	nodeConnectionQueue middleware.MessageMiddleware
	dataQueue           middleware.MessageMiddleware
	messageHandlers     map[enum.TaskType]func([]byte) error
	workerName          string
	stopConsuming       chan bool
}

func NewMessageHandler(
	Address string,
) *MessageHandler {

	workerName := fmt.Sprintf("%s_%s", GroupByPrefix, uuid.New().String())
	mh := &MessageHandler{
		reduceSumQueue:      middleware.GetReduceSumQueue(Address),
		reduceCountQueue:    middleware.GetReduceCountQueue(Address),
		nodeConnectionQueue: middleware.GetNodeConnectionsQueue(Address),
		dataQueue:           middleware.GetDataExchange(Address, []string{GroupByPrefix, workerName}),
		workerName:          workerName,
	}

	mh.stopConsuming = make(chan bool, 1)

	return mh
}

func (mh *MessageHandler) Close() error {

	mh.stopConsuming <- true

	e := mh.reduceSumQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing reduce sum queue middleware: %v", e)
	}

	e = mh.reduceCountQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing reduce count queue middleware: %v", e)
	}

	e = mh.nodeConnectionQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing controller connection middleware: %v", e)
	}

	e = mh.dataQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing finish exchange middleware: %v", e)
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
) error {
	defer mh.dataQueue.StopConsuming()

	isFirstMessage := true
	mh.dataQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
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

func (mh *MessageHandler) SendData(taskType enum.TaskType, payload []byte) error {
	return mh.sendToQueues(taskType, payload, mh.joinerQueue)
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
