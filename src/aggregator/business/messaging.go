package service

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/handler"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/controller_connection"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type MessageHandler func(msgBody []byte) error

func StartConsumer(m middleware.MessageMiddleware, handler MessageHandler) error {
	err := startConsuming(m, handler)
	if err != nil {
		_ = StopConsumer(m)
		return err
	}
	return nil
}

func StartDirectExchange(finishExchange middleware.MessageMiddleware, handler MessageHandler) error {
	err := startConsuming(finishExchange, handler)
	if err != nil {
		_ = StopConsumer(finishExchange)
		return err
	}
	return nil
}

func StopConsumers(middlewares MessageMiddlewares) (MessageMiddlewares, error) {
	for _, midd := range middlewares {
		if midd != nil {
			if err := StopConsumer(midd); err != nil {
				return nil, err
			}
		}
	}
	return make(MessageMiddlewares), nil
}

func StopConsumer(m middleware.MessageMiddleware) error {
	if m.StopConsuming() != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to stop consuming")
	}
	if m.Delete() != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to delete middleware")
	}
	return nil
}

func StopSender(m middleware.MessageMiddleware) (middleware.MessageMiddleware, error) {
	if m != nil {
		if m.Delete() != middleware.MessageMiddlewareSuccess {
			return m, fmt.Errorf("failed to delete middleware")
		}
	}
	return nil, nil
}

func startConsuming(m middleware.MessageMiddleware, handler MessageHandler) error {
	e := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			handlerErr := handler(msg.Body)
			if handlerErr != nil {
				_ = msg.Nack(false, false)
				log.Errorf("Handler returned error: %v", handlerErr)
				return
			}
			_ = msg.Ack(false)
		}
		d <- nil
	})

	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("StartConsuming returned error code %d", int(e))
	}

	return nil
}

func SendBatchToGateway(batch proto.Message, gatewayQueue middleware.MessageMiddleware, taskType enum.TaskType) error {
	payload, marshalErr := proto.Marshal(batch)
	if marshalErr != nil {
		return marshalErr
	}

	dataBatch := &data_batch.DataBatch{
		TaskType: int32(taskType),
		Done:     false,
		Payload:  payload,
	}

	sendErr := sendDataBatch(dataBatch, gatewayQueue)
	if sendErr != nil {
		return sendErr
	}

	return nil
}

func SendDoneBatchToGateway(gatewayQueue middleware.MessageMiddleware, taskType enum.TaskType, clientId string) error {
	dataBatch := &data_batch.DataBatch{
		TaskType: int32(taskType),
		ClientId: clientId,
		Done:     true,
	}

	sendErr := sendDataBatch(dataBatch, gatewayQueue)
	if sendErr != nil {
		return sendErr
	}
	log.Debugf("Sent done batch for task %d to gateway", taskType)

	return nil
}

func sendDataBatch(dataBatch *handler.DataBatch, queue middleware.MessageMiddleware) error {
	dataBytes, err := proto.Marshal(dataBatch)
	if err != nil {
		return err
	}

	e := queue.Send(dataBytes)
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("StartConsuming returned error code %d", int(e))
	}

	return nil
}

func SendControllerConnectionMsg(controllerConnection middleware.MessageMiddleware, workerName string, clientId string, isFinished bool) error {
	announceMsg := &controller_connection.ControllerConnection{
		WorkerName: workerName,
		ClientId:   clientId,
		Finished:   isFinished,
	}

	msgBytes, err := proto.Marshal(announceMsg)
	if err != nil {
		return err
	}

	e := controllerConnection.Send(msgBytes)
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("StartConsuming returned error code %d", int(e))
	}

	return nil
}
