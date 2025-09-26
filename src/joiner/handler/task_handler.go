package handler

import (
	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
)

type TaskHandlers map[models.TaskType]HandleTask

type TaskHandler struct {
	taskHandlers TaskHandlers
}

func NewTaskHandler() *TaskHandler {
	th := &TaskHandler{}

	th.taskHandlers = TaskHandlers{
		models.T2: th.handleTaskType2,
		models.T3: th.handleTaskType3,
		models.T4: th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType models.TaskType) HandleTask {
	return th.taskHandlers[taskType]
}

func (th *TaskHandler) handleTaskType2(dataBatch *protocol.DataBatch) error {
	return nil
}

func (th *TaskHandler) handleTaskType3(dataBatch *protocol.DataBatch) error {
	return nil
}

func (th *TaskHandler) handleTaskType4(dataBatch *protocol.DataBatch) error {
	return nil
}
