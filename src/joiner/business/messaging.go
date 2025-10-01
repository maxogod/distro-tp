package service

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/controller_connection"
	"google.golang.org/protobuf/proto"
)

type MessageHandler func(msgBody []byte) error
type MessageExchange func() error

func StartConsumer(gatewayAddress, queueName string, handler MessageHandler) (middleware.MessageMiddleware, error) {
	m, err := middleware.NewQueueMiddleware(gatewayAddress, queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to start queue middleware: %w", err)
	}

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
		return nil, fmt.Errorf("StartConsuming returned error code %d", int(e))
	}

	return m, nil
}

func StopConsumers(middlewares MessageMiddlewares) (MessageMiddlewares, error) {
	for _, midd := range middlewares {
		if err := StopConsumer(midd); err != nil {
			return nil, err
		}
	}
	return make(MessageMiddlewares), nil
}

func StopSenders(middlewares MessageMiddlewares) (MessageMiddlewares, error) {
	for _, midd := range middlewares {
		if err := StopSender(midd); err != nil {
			return nil, err
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

func StartSender(gatewayAddress, queueName string) (middleware.MessageMiddleware, error) {
	m, err := middleware.NewQueueMiddleware(gatewayAddress, queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to start queue middleware: %w", err)
	}

	return m, nil
}

func StopSender(m middleware.MessageMiddleware) error {
	if m.Delete() != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to delete middleware")
	}
	return nil
}

func SendMessageToControllerConnection(
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

func StartDirectExchange(finishExchange middleware.MessageMiddleware, handler MessageExchange) error {
	e := finishExchange.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			handlerErr := handler()
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
