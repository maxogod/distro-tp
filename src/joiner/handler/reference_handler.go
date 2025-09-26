package handler

import (
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"github.com/maxogod/distro-tp/src/joiner/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type ReferenceHandler struct {
	handlerDone func(msg *amqp.Delivery, queueName string, taskType int32)
	queue       string
	storeDir    string
}

func NewReferenceHandler(handlerDone func(msg *amqp.Delivery, queueName string, taskType int32), queueName, storeDir string) *ReferenceHandler {
	return &ReferenceHandler{handlerDone: handlerDone, queue: queueName, storeDir: storeDir}
}

func (h *ReferenceHandler) HandleReferenceQueueMessage(msg *amqp.Delivery) {
	var refMsg protocol.ReferenceQueueMessage
	if err := proto.Unmarshal(msg.Body, &refMsg); err != nil {
		_ = msg.Nack(false, false)
		return
	}

	switch payload := refMsg.Payload.(type) {
	case *protocol.ReferenceQueueMessage_ReferenceBatch:
		cache.StoreReferenceData(h.storeDir, msg, payload.ReferenceBatch)
	case *protocol.ReferenceQueueMessage_Done:
		h.handlerDone(msg, h.queue, payload.Done.TaskType)
	default:
		_ = msg.Nack(false, false)
	}
}
