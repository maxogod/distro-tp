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
	config              *config.Config
	referenceMiddleware middleware.MessageMiddleware
	dataMiddleware      middleware.MessageMiddleware
}

func NewJoiner(config *config.Config) *Joiner {
	return &Joiner{
		config: config,
	}
}

func (j *Joiner) StartRefConsumer(referenceDatasetQueue string) error {
	m, queueErr := middleware.NewQueueMiddleware(j.config.GatewayAddress, referenceDatasetQueue)
	if queueErr != nil {
		return fmt.Errorf("failed to start queue middleware: %w", queueErr)
	}
	j.referenceMiddleware = m

	e := j.referenceMiddleware.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			j.handleMessage(&msg)
		}
		d <- nil
	})

	if int(e) != 0 {
		return fmt.Errorf("StartConsuming returned error code %d", int(e))
	}

	return nil
}

func (j *Joiner) handleMessage(msg *amqp.Delivery) {
	var refQueueMsg protocol.ReferenceQueueMessage
	if err := proto.Unmarshal(msg.Body, &refQueueMsg); err != nil {
		_ = msg.Nack(false, false)
		return
	}

	switch payload := refQueueMsg.Payload.(type) {
	case *protocol.ReferenceQueueMessage_ReferenceBatch:
		joinerUtils.StoreReferenceData(j.config.StorePath, msg, payload.ReferenceBatch)
	case *protocol.ReferenceQueueMessage_Done:
		err := j.startDataConsumer(msg)
		if err != nil {
			_ = msg.Nack(false, false)
			return
		}
	default:
		// Unknown message
		_ = msg.Nack(false, false)
	}
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
	if j.referenceMiddleware != nil {
		if err := j.referenceMiddleware.StopConsuming(); err == middleware.MessageMiddlewareMessageError {
			return fmt.Errorf("failed to stop consuming")
		}
		if err := j.referenceMiddleware.Close(); err == middleware.MessageMiddlewareMessageError {
			return fmt.Errorf("failed to close middleware")
		}
	}
	return nil
}

func (j *Joiner) handleTaskType3(msg amqp.Delivery) {

}
