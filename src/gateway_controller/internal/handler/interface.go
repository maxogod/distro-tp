package handler

import (
	"time"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
)

const (
	RECEIVING_TIMEOUT = 5 * time.Second
)

// MessageHandler interface defines methods for forwarding tasks to be processed by workers
// and managing client interactions.
// Messaging methods need the current clientID to support multiclient environments.
type MessageHandler interface {

	// ForwardData sends a given data envelope to the corresponding worker layer to start processing it.
	ForwardData(dataBatch *protocol.DataEnvelope) error

	// ForwardReferenceData sends a given reference data envelope to the corresponding worker layer to
	// use it for data merging.
	ForwardReferenceData(dataBatch *protocol.DataEnvelope) error

	// SendDone notifies the
	SendDone(worker enum.WorkerType) error

	// GetReportData generates data envelopes received from workers into the provided channel.
	// Ignoring any messages that do not match the given clientID.
	GetReportData(data chan *protocol.DataEnvelope)

	// Close releases any resources held by the handler.
	// e.g. middleware queues or exchanges instantiation.
	Close()
}
