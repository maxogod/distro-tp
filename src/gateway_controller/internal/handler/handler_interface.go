package handler

import (
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

// TODO: DOCUMENT THIS INTERFACE
type Handler interface {
	HandleTask(taskType enum.TaskType, dataBatch *data_batch.DataBatch) error
	HandleReferenceData(dataBatch *data_batch.DataBatch, clientID string) error
	SendDone(taskType enum.TaskType, currentClientID string) error
	GetReportData(data chan []byte, disconnect chan bool)
	Close()
}
