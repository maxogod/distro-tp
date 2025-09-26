package service

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
)

type MessageHandler func(msgBody []byte) error

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

	if int(e) != 0 {
		return nil, fmt.Errorf("StartConsuming returned error code %d", int(e))
	}

	return m, nil
}

func StopConsumer(m middleware.MessageMiddleware) error {
	if m.StopConsuming() != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to stop consuming")
	}
	if m.Close() != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close middleware")
	}
	return nil
}
