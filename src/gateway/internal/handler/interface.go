package handler

import (
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
)

// MessageHandler interface defines methods for forwarding tasks to be processed by workers
// and managing client interactions.
type MessageHandler interface {

	// SendControllerInit comunicates with the controller to start a session
	SendControllerInit(taskType enum.TaskType) error

	// AwaitControllerInit waits until the controller acknowledges the session start.
	AwaitControllerInit() error

	// NotifyClientMessagesCount notifies the controller about the total number of messages
	// that was sent by the client for processing.
	NotifyClientMessagesCount() error

	// NotifyCompletion informs the controller that all processed data was received and forwarded.
	NotifyCompletion(clientId string) error

	// ForwardData sends a given data envelope to the corresponding worker layer to start processing it.
	ForwardData(dataBatch *protocol.DataEnvelope) error

	// ForwardReferenceData sends a given reference data envelope to the corresponding worker layer to
	// use it for data merging.
	ForwardReferenceData(dataBatch *protocol.DataEnvelope) error

	// GetReportData generates data envelopes received from workers into the provided channel.
	// Ignoring any messages that do not match the given clientID.
	GetReportData(data chan *protocol.DataEnvelope)

	// Close releases any resources held by the handler.
	// e.g. middleware queues or exchanges instantiation.
	Close()
}
