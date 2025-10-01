package workers_manager

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

type WorkerExistsError struct{}

func (w *WorkerExistsError) Error() string {
	return "worker already exists in WorkersManager"
}

type WorkerNotExistsError struct{}

func (w *WorkerNotExistsError) Error() string {
	return "worker does not exist in WorkersManager"
}

type InvalidWorkerTypeError struct {
	workerType string
}

func (w *InvalidWorkerTypeError) Error() string {
	return fmt.Sprintf("invalid worker type: %s", w.workerType)
}

// WorkersManager is an interface for managing worker nodes in the distributed pipeline.
type WorkersManager interface {
	// AddWorker adds a new worker node with the given UUID.
	AddWorker(id string) error

	// FinishWorker marks the worker node with the given UUID as finished.
	FinishWorker(id string) error

	// GetFinishExchangeTopic returns the topic name for finishing exchange that should be used
	// based on the amount of finished workers and the total number of workers for one specific stage.
	// Also returns a boolean indicating whether the last stage was finished or not.
	GetFinishExchangeTopic() (enum.WorkerType, bool)

	// GetWorkerConnectionRR returns a FILTER worker middleware connection in a round-robin fashion.
	GetWorkerConnectionRR() (middleware.MessageMiddleware, error)
}
