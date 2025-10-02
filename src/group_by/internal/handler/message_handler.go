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

type MessageHandler struct {
	// connections
	groupByQueue        middleware.MessageMiddleware
	reducerQueue        middleware.MessageMiddleware
	nodeConnectionQueue middleware.MessageMiddleware

	// internals
	currentClientID string
	messageHandlers map[enum.TaskType]func([]byte) error
	workerName      string
	stopConsuming   chan bool
}

func NewMessageHandler(
	Address string,
) *MessageHandler {

	workerName := fmt.Sprintf("%s_%s", string(enum.GroupBy), uuid.New().String())
	mh := &MessageHandler{
		groupByQueue:        middleware.GetGroupByQueue(Address),
		reducerQueue:        middleware.GetReducerQueue(Address),
		nodeConnectionQueue: middleware.GetNodeConnectionsQueue(Address),
		workerName:          workerName,
	}

	mh.stopConsuming = make(chan bool, 1)

	mh.messageHandlers = map[enum.TaskType]func([]byte) error{
		enum.T2: mh.sendT2Data,
		enum.T3: mh.sendT3Data,
		enum.T4: mh.sendT4Data,
	}

	return mh
}

func (mh *MessageHandler) Close() error {

	mh.stopConsuming <- true

	e := mh.reducerQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing reduce sum queue middleware: %v", e)
	}

	e = mh.nodeConnectionQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing controller connection middleware: %v", e)
	}

	e = mh.groupByQueue.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing group by queue middleware: %v", e)
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
	defer mh.groupByQueue.StopConsuming()

	isFirstMessage := true
	mh.groupByQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
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
				log.Debugf("Payload length: %d", len(payload))
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

	err := handler(serializedFilteredTransactions)
	if err != nil {
		return err
	}

	return nil
}

func (mh *MessageHandler) sendT2Data(payload []byte) error {
	return mh.sendToQueues(enum.T2, payload, mh.reducerQueue)
}

func (mh *MessageHandler) sendT3Data(payload []byte) error {
	return mh.sendToQueues(enum.T3, payload, mh.reducerQueue)
}

func (mh *MessageHandler) sendT4Data(payload []byte) error {
	return mh.sendToQueues(enum.T4, payload, mh.reducerQueue)
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
