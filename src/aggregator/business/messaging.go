package service

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/handler"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/controller_connection"
	"google.golang.org/protobuf/proto"
)

type MessageHandler func(msgBody []byte) error

func StartConsumer(gatewayAddress, queueName string, handler MessageHandler) (middleware.MessageMiddleware, error) {
	m, err := StartQueueMiddleware(gatewayAddress, queueName)
	if err != nil {
		return nil, err
	}

	err = startConsuming(m, handler)
	if err != nil {
		_ = StopConsumer(m)
		return nil, err
	}

	return m, nil
}

func StartQueueMiddleware(gatewayAddress, gatewayControllerQueue string) (middleware.MessageMiddleware, error) {
	m, err := middleware.NewQueueMiddleware(gatewayAddress, gatewayControllerQueue)
	if err != nil {
		return nil, fmt.Errorf("failed to start queue middleware: %w", err)
	}
	return m, nil
}

func StartDirectExchange(gatewayAddress, exchangeName, routingKey string, handler MessageHandler) (middleware.MessageMiddleware, error) {
	finishExchange, err := middleware.NewExchangeMiddleware(gatewayAddress, exchangeName, "direct", []string{routingKey})

	if err != nil {
		return nil, fmt.Errorf("failed to start queue middleware: %w", err)
	}

	err = startConsuming(finishExchange, handler)
	if err != nil {
		_ = StopConsumer(finishExchange)
		return nil, err
	}

	return finishExchange, nil
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
				continue
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

func SendDataBatch(dataBatch *handler.DataBatch, queue middleware.MessageMiddleware) error {
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

func SendControllerConnectionMsg(
	controllerConnection middleware.MessageMiddleware,
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

	e := controllerConnection.Send(msgBytes)
	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("StartConsuming returned error code %d", int(e))
	}

	return nil
}
