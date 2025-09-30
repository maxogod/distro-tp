package handler

import (
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

type TaskHandlers map[enum.TaskType]HandleTask

type TaskHandler struct {
	taskHandlers    TaskHandlers
	refDatasetStore *cache.ReferenceDatasetStore
}

func NewTaskHandler(referenceDatasetStore *cache.ReferenceDatasetStore) *TaskHandler {
	th := &TaskHandler{
		refDatasetStore: referenceDatasetStore,
	}

	th.taskHandlers = TaskHandlers{
		enum.T1: th.handleTaskType1,
		enum.T2: th.handleTaskType2,
		enum.T3: th.handleTaskType3,
		enum.T4: th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType enum.TaskType) HandleTask {
	return th.taskHandlers[taskType]
}

func (th *TaskHandler) handleTaskType1(dataBatch *data_batch.DataBatch) error {
	return nil
}

func (th *TaskHandler) handleTaskType2(dataBatch *data_batch.DataBatch) error {
	return nil
}

func (th *TaskHandler) handleTaskType3(dataBatch *data_batch.DataBatch) error {
	return nil
}

func (th *TaskHandler) handleTaskType4(dataBatch *data_batch.DataBatch) error {
	return nil
}
