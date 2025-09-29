package handler

import (
	"github.com/maxogod/distro-tp/src/common/protocol"
	"google.golang.org/protobuf/proto"
)

type DataBatch = protocol.DataBatch
type HandleTask = func(dataBatch *DataBatch) error

type DataHandler struct {
	handlerTask HandleTask
}

func NewDataHandler(handler HandleTask) *DataHandler {
	return &DataHandler{
		handlerTask: handler,
	}
}

func (h *DataHandler) HandleDataMessage(msgBody []byte) error {
	var batch DataBatch
	if err := proto.Unmarshal(msgBody, &batch); err != nil {
		return err
	}
	return h.handlerTask(&batch)
}
