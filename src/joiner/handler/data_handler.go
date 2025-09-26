package handler

import (
	"github.com/maxogod/distro-tp/src/joiner/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type DataBatch = protocol.DataBatch

type DataHandler struct {
	handler func(dataBatch *DataBatch)
}

func NewDataHandler(handler func(dataBatch *DataBatch)) *DataHandler {
	return &DataHandler{handler: handler}
}

func (h *DataHandler) HandleDataMessage(msg *amqp.Delivery) {
	var batch DataBatch
	if err := proto.Unmarshal(msg.Body, &batch); err != nil {
		_ = msg.Nack(false, false)
		return
	}
	h.handler(&batch)
	_ = msg.Ack(false)
}
