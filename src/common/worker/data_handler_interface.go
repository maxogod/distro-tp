package worker

import "github.com/maxogod/distro-tp/src/common/models/protocol"

// This interface is required for workers to use when attempting to handle data,
// This must go in tandem with the MessageHandler struct
type DataHandler interface {
	HandleData(dataEnvelope *protocol.DataEnvelope) error
	HandleFinishClient(clientID string) error
	Close() error
}
