package handler

import (
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
)

// Handler interface defines methods for processing tasks and managing client interactions.
type Handler interface {

	// HandleTask processes a given task type with the provided data batch.
	// It sends the data to the corresponding workers.
	HandleTask(taskType enum.TaskType, dataBatch *protocol.DataEnvelope) error

	// HandleReferenceData processes reference data sent by a client.
	// It sends the data to the joiner workers.
	HandleReferenceData(dataBatch *protocol.DataEnvelope, clientID string) error

	// SendDone notifies that the client has finished sending data for a specific task type.
	// It sends a done signal to the corresponding workers.
	SendDone(taskType enum.TaskType, currentClientID string) error

	// GetReportData retrieves report data from workers and sends it to the provided channel.
	// It also listens for a disconnect signal to stop the operation (e.g. when Done is received).
	GetReportData(data chan []byte, disconnect chan bool)

	// Reset clears the internal state of the handler, preparing it for a new session.
	Reset()

	// Close releases any resources held by the handler.
	Close()
}
