package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

var log = logger.GetLogger()

type TaskHandler struct {
	WorkerService *business.WorkerService
	taskHandlers  map[enum.TaskType]func(any) (any, error)
}

func NewTaskHandler() *TaskHandler {
	th := &TaskHandler{
		WorkerService: business.NewWorkerService(),
	}

	th.taskHandlers = map[enum.TaskType]func(any) (any, error){
		enum.T1: th.handleTaskType1,
		enum.T2: th.handleTaskType2,
		enum.T3: th.handleTaskType3,
		enum.T4: th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType enum.TaskType, payload any) (any, error) {
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
