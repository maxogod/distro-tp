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

const workerNamePrefix = "filter"

type MessageHandler struct {
	filterQueueMiddleware      middleware.MessageMiddleware
	reduceSumQueueMiddleware   middleware.MessageMiddleware
	reduceCountQueueMiddleware middleware.MessageMiddleware
	processDataQueueMiddleware middleware.MessageMiddleware
	controllerConnection       middleware.MessageMiddleware
	finishExchangeMiddleware   middleware.MessageMiddleware
	messageHandlers            map[enum.TaskType]func([]byte) error
	workerName                 string
	finishFilter               chan bool
}

func NewMessageHandler(
	filterQueueMiddleware middleware.MessageMiddleware,
	reduceSumQueueMiddleware middleware.MessageMiddleware,
	reduceCountQueueMiddleware middleware.MessageMiddleware,
	processDataQueueMiddleware middleware.MessageMiddleware,
	controllerConnection middleware.MessageMiddleware,
	finishExchangeMiddleware middleware.MessageMiddleware,
) *MessageHandler {

	mh := &MessageHandler{
		filterQueueMiddleware:      filterQueueMiddleware,
		reduceSumQueueMiddleware:   reduceSumQueueMiddleware,
		reduceCountQueueMiddleware: reduceCountQueueMiddleware,
		processDataQueueMiddleware: processDataQueueMiddleware,
		controllerConnection:       controllerConnection,
		finishExchangeMiddleware:   finishExchangeMiddleware,
	}

	mh.workerName = fmt.Sprintf("%s-%s", workerNamePrefix, uuid.New().String())
	mh.finishFilter = make(chan bool, 1)

	mh.messageHandlers = map[enum.TaskType]func([]byte) error{
		enum.T1: mh.sendT1Data,
		enum.T2: mh.sendT2Data,
		enum.T3: mh.sendT3Data,
		enum.T4: mh.sendT4Data,
	}

	return mh
}

func (qh *MessageHandler) Close() error {
	e := qh.filterQueueMiddleware.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing filter queue middleware: %v", e)
	}

	e = qh.reduceSumQueueMiddleware.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing reduce sum queue middleware: %v", e)
	}

	e = qh.reduceCountQueueMiddleware.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing reduce count queue middleware: %v", e)
	}

	e = qh.processDataQueueMiddleware.Close()
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Error closing process data queue middleware: %v", e)
	}
	return nil
}

func (mh *MessageHandler) AnnounceToController() error {

	e := mh.sendMessageToControllerConnection(
		mh.workerName,
		false,
	)
	if e != nil {
		return e
	}
	return nil
}

func (mh *MessageHandler) listenForDone() error {

	e := mh.finishExchangeMiddleware.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, err := utils.GetDataBatch(msg.Body)
			if err != nil {
				log.Errorf("Failed to get data batch: %v", err)
				continue
			}
			done := dataBatch.GetDone()

			if done {
				log.Debug("Received done message. Finishing processing.")
				mh.finishFilter <- true
				return
			}
			d <- nil
			return
		}
	})

	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Failed to start consuming finish exchange: %d", e)
	}

	return nil
}

func (mh *MessageHandler) StartReceiving(
	callback func(payload []byte, taskType int32) error,
) middleware.MessageMiddlewareError {

	go func() {
		_ = mh.listenForDone()
	}()

	doneChan := mh.finishFilter
	return mh.filterQueueMiddleware.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for {
			select {
			case <-doneChan:
				log.Info("Received done signal, stopping message consumption.")
				return
			case msg, ok := <-consumeChannel:
				if !ok {
					return
				}
				msg.Ack(false)
				dataBatch, err := utils.GetDataBatch(msg.Body)
				if err != nil {
					log.Errorf("Failed to get data batch: %v", err)
					continue
				}
				payload := dataBatch.GetPayload()
				taskType := dataBatch.GetTaskType()

				err = callback(payload, taskType)
				if err != nil {
					log.Errorf("Failed to process message: %v", err)
					continue
				}
			}
		}
	})
}

func (mh *MessageHandler) SendDone() error {

	e := mh.sendMessageToControllerConnection(
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

func (mh *MessageHandler) sendT1Data(payload []byte) error {
	return mh.sendToQueues(enum.T1, payload, mh.processDataQueueMiddleware)
}

func (mh *MessageHandler) sendT2Data(payload []byte) error {
	return mh.sendToQueues(enum.T2, payload, mh.reduceCountQueueMiddleware, mh.reduceSumQueueMiddleware)
}

func (mh *MessageHandler) sendT3Data(payload []byte) error {
	return mh.sendToQueues(enum.T3, payload, mh.reduceSumQueueMiddleware)
}

func (mh *MessageHandler) sendT4Data(payload []byte) error {
	return mh.sendToQueues(enum.T4, payload, mh.reduceCountQueueMiddleware)
}

func (mh *MessageHandler) sendToQueues(
	taskType enum.TaskType,
	payload []byte,
	queues ...middleware.MessageMiddleware,
) error {
	dataBatch := &data_batch.DataBatch{
		TaskType: int32(taskType),
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

func (mh *MessageHandler) sendMessageToControllerConnection(
	workerName string,
	isFinished bool,
) error {
	announceMsg := &controller_connection.ControllerConnection{
		WorkerName: workerName,
		Finished:   isFinished,
	}
	msgBytes, err := proto.Marshal(announceMsg)
	if err != nil {
		return err
	}

	e := mh.controllerConnection.Send(msgBytes)
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("A error occurred while sending message to controller connection: %d", int(e))
	}

	return nil
}
