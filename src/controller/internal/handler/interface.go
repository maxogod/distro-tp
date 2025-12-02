package handler

import (
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

// ControlHandler interface defines methods for forwarding tasks to be processed by workers
// and managing client interactions.
// Messaging methods need the current clientID to support multiclient environments.
type ControlHandler interface {

	// AwaitForWorkers blocks until all workers have signaled completion for the current clientID.
	AwaitForWorkers() error

	// SendDone notifies the
	SendDone(worker enum.WorkerType, totalMsgs int, deleteAction bool) error

	// Close releases any resources held by the handler.
	// e.g. middleware queues or exchanges instantiation.
	Close()

	// SendControllerReady notifies the gateway that the controller is ready to receive messages
	SendControllerReady()
}
