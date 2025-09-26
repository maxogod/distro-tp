package handler

import (
	"github.com/maxogod/distro-tp/src/common/protocol"
	"google.golang.org/protobuf/proto"
)

type DataBatch = protocol.DataBatch
type HandleTask = func(dataBatch *DataBatch, refDatasetDir string) error

type DataHandler struct {
	handlerTask   HandleTask
	refDatasetDir string
}

func NewDataHandler(handler HandleTask, refDatasetDir string) *DataHandler {
	return &DataHandler{
		handlerTask:   handler,
		refDatasetDir: refDatasetDir,
	}
}

func (h *DataHandler) HandleDataMessage(msgBody []byte) error {
	var batch DataBatch
	if err := proto.Unmarshal(msgBody, &batch); err != nil {
		return err
	}
	return h.handlerTask(&batch, h.refDatasetDir)
}
