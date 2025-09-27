package handler

import (
	"coffee-analisis/src/common/models"
	"coffee-analisis/src/worker_base/business"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("task-handler")

type TaskHandler struct {
	WorkerService *business.WorkerService
	taskHandlers  map[models.TaskType]func(any) (any, error)
}

func NewTaskHandler() *TaskHandler {
	th := &TaskHandler{
		WorkerService: business.NewWorkerService(),
	}

	th.taskHandlers = map[models.TaskType]func(any) (any, error){
		models.T1: th.handleTaskType1,
		models.T2: th.handleTaskType2,
		models.T3: th.handleTaskType3,
		models.T4: th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType models.TaskType, payload any) (any, error) {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		return nil, fmt.Errorf("unknown task type: %d", taskType)
	}

	log.Infof("Processing task type: %d", taskType)
	return handler(payload)
}

func (th *TaskHandler) handleTaskType1(payload any) (any, error) {
	log.Info("Handling Task Type 1")

	return nil, nil
}

func (th *TaskHandler) handleTaskType2(payload any) (any, error) {
	log.Info("Handling Task Type 2")

	return nil, nil
}

func (th *TaskHandler) handleTaskType3(payload any) (any, error) {
	log.Info("Handling Task Type 3")

	return nil, nil
}

func (th *TaskHandler) handleTaskType4(payload any) (any, error) {
	log.Info("Handling Task Type 4")

	return nil, nil
}
