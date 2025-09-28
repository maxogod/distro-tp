package handler

import (
	"github.com/maxogod/distro-tp/src/common/protocol"
	"google.golang.org/protobuf/proto"
)

type DataBatch = protocol.DataBatch
type HandleTask = func(dataBatch *DataBatch, refDatasetDir string, isBestSellingTask bool) error

type DataHandler struct {
	handlerTask       HandleTask
	refDatasetDir     string
	isBestSellingTask bool
}

func NewDataHandler(handler HandleTask, refDatasetDir string, isBestSellingTask bool) *DataHandler {
	return &DataHandler{
		handlerTask:       handler,
		refDatasetDir:     refDatasetDir,
		isBestSellingTask: isBestSellingTask,
	}
}

func (h *DataHandler) HandleDataMessage(msgBody []byte) error {
	var batch DataBatch
	if err := proto.Unmarshal(msgBody, &batch); err != nil {
		return err
	}
	return h.handlerTask(&batch, h.refDatasetDir, h.isBestSellingTask)
}
