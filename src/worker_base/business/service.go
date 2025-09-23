package business

import "github.com/maxogod/distro-tp/common/logger"

var log = logger.GetLogger()

type WorkerService struct{}

// NewWorkerService creates a new instance of WorkerService
func NewWorkerService() *WorkerService {
	return &WorkerService{}
}

// HandleTask processes a task and prints a message
func (ws *WorkerService) HandleTask() {
	log.Info("Task being processed... | Implement your task logic here")
}
