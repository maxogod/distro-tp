package business

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/protocol"
	joinerUtils "github.com/maxogod/distro-tp/src/joiner/utils"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type Joiner struct {
	config               *config.Config
	referenceMiddlewares map[string]middleware.MessageMiddleware
	dataMiddleware       middleware.MessageMiddleware
}

func NewJoiner(config *config.Config) *Joiner {
	return &Joiner{
		config:               config,
		referenceMiddlewares: make(map[string]middleware.MessageMiddleware),
	}
}

func (j *Joiner) StartRefConsumer(referenceDatasetQueue string) error {
	m, queueErr := middleware.NewQueueMiddleware(j.config.GatewayAddress, referenceDatasetQueue)
	if queueErr != nil {
		return fmt.Errorf("failed to start queue middleware: %w", queueErr)
	}
	j.referenceMiddlewares[referenceDatasetQueue] = m

	e := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			j.handleMessage(&msg, referenceDatasetQueue)
		}
		d <- nil
	})

	if int(e) != 0 {
		return fmt.Errorf("StartConsuming returned error code %d", int(e))
	}

	return nil
}

func (j *Joiner) handleMessage(msg *amqp.Delivery, queueName string) {
	var refQueueMsg protocol.ReferenceQueueMessage
	if err := proto.Unmarshal(msg.Body, &refQueueMsg); err != nil {
		_ = msg.Nack(false, false)
		return
	}

	switch payload := refQueueMsg.Payload.(type) {
	case *protocol.ReferenceQueueMessage_ReferenceBatch:
		joinerUtils.StoreReferenceData(j.config.StorePath, msg, payload.ReferenceBatch)
	case *protocol.ReferenceQueueMessage_Done:
		stopErr := j.stopRefConsumer(queueName)
		if stopErr != nil {
			_ = msg.Nack(false, false)
			return
		}

		startErr := j.startDataConsumer(msg)
		if startErr != nil {
			_ = msg.Nack(false, false)
			return
		}
	default:
		// Unknown message
		_ = msg.Nack(false, false)
	}
}

func (j *Joiner) stopRefConsumer(queueName string) error {
	stopErr := j.stopRefMiddleware(j.referenceMiddlewares[queueName])
	if stopErr != nil {
		return stopErr
	}
	delete(j.referenceMiddlewares, queueName)
	return nil
}

func (j *Joiner) startDataConsumer(msg *amqp.Delivery) error {
	m, queueErr := middleware.NewQueueMiddleware(j.config.GatewayAddress, j.config.StoreTPVQueue)
	if queueErr != nil {
		return fmt.Errorf("failed to start queue middleware: %w", queueErr)
	}
	j.dataMiddleware = m
	_ = msg.Ack(false)

	e := j.dataMiddleware.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for dataMsg := range consumeChannel {
			j.handleTaskType3(dataMsg)
			_ = dataMsg.Ack(false)
		}
		d <- nil
	})

	if int(e) != 0 {
		return fmt.Errorf("StartConsuming returned error code %d", int(e))
	}

	return nil
}

func (j *Joiner) Stop() error {
	for _, referenceMiddleware := range j.referenceMiddlewares {
		err := j.stopRefMiddleware(referenceMiddleware)
		if err != nil {
			return err
		}
	}
	return nil
}

func (j *Joiner) handleTaskType3(msg amqp.Delivery) {

}

func (j *Joiner) stopRefMiddleware(referenceMiddleware middleware.MessageMiddleware) error {
	if referenceMiddleware.StopConsuming() != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to stop consuming")
	}
	if referenceMiddleware.Close() != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to close middleware")
	}

	return nil
}
