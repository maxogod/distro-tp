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
	currentClientID     string
	groupbyQueue        middleware.MessageMiddleware
	aggregatorQueue     middleware.MessageMiddleware
	nodeConnectionQueue middleware.MessageMiddleware
	dataQueue           middleware.MessageMiddleware
	messageHandlers     map[enum.TaskType]func(enum.TaskType, []byte) error
	workerName          string
	stopConsuming       chan bool
}

func NewMessageHandler(
	Address string,
) *MessageHandler {

	workerName := fmt.Sprintf("%s_%s", FilterPrefix, uuid.New().String())
	mh := &MessageHandler{
		groupbyQueue:        middleware.GetGroupByQueue(Address),
		aggregatorQueue:     middleware.GetFilteredTransactionsQueue(Address),
		nodeConnectionQueue: middleware.GetNodeConnectionsQueue(Address),
		dataQueue:           middleware.GetDataExchange(Address, []string{FilterPrefix, workerName}),
		workerName:          workerName,
	}

	mh.stopConsuming = make(chan bool, 1)

	mh.messageHandlers = map[enum.TaskType]func(enum.TaskType, []byte) error{
		enum.T1: mh.sendToAggregator,
		enum.T2: mh.sendToGroupBy,
		enum.T3: mh.sendToGroupBy,
		enum.T4: mh.sendToGroupBy,
	}

	return mh
}

func (mh *MessageHandler) Close() error {

	mh.stopConsuming <- true

	e := mh.groupbyQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing group by queue middleware: %v", e)
	}

	e = mh.aggregatorQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing process data queue middleware: %v", e)
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

func (mh *MessageHandler) SendData(taskType enum.TaskType, serializedFilteredTransactions []byte) error {
	handler, exists := mh.messageHandlers[taskType]
	if !exists {
		return fmt.Errorf("unknown task type: %d", taskType)
	}

	err := handler(taskType, serializedFilteredTransactions)
	if err != nil {
		return err
	}

	return nil
}

func (mh *MessageHandler) sendToAggregator(taskType enum.TaskType, payload []byte) error {
	return mh.sendToQueues(taskType, payload, mh.aggregatorQueue)
}

func (mh *MessageHandler) sendToGroupBy(taskType enum.TaskType, payload []byte) error {
	return mh.sendToQueues(taskType, payload, mh.groupbyQueue)
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
