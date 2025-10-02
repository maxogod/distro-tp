package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

var log = logger.GetLogger()

type TaskHandler struct {
	taskHandlers    map[enum.TaskType]func(*data_batch.DataBatch) error
	refDatasetStore *cache.DataBatchStore
}

func NewTaskHandler(
	referenceDatasetStore *cache.DataBatchStore,
) *TaskHandler {
	th := &TaskHandler{
		refDatasetStore: referenceDatasetStore,
	}

	th.taskHandlers = map[enum.TaskType]func(*data_batch.DataBatch) error{
		enum.T1:   th.handleTaskType1,
		enum.T2_1: th.handleTaskType2_1,
		enum.T2_2: th.handleTaskType2_2,
		enum.T3:   th.handleTaskType3,
		enum.T4:   th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(data *data_batch.DataBatch) error {
	handler, exists := th.taskHandlers[enum.TaskType(data.GetTaskType())]
	if !exists {
		return fmt.Errorf("unknown task type: %d", data.GetTaskType())
	}
	return handler(data)
}

func (th *TaskHandler) handleTaskType1(data *data_batch.DataBatch) error {
	err := th.refDatasetStore.StoreDataTask1(data)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType2_1(data *data_batch.DataBatch) error {
	err := th.refDatasetStore.StoreDataBestSelling(data)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType2_2(data *data_batch.DataBatch) error {
	err := th.refDatasetStore.StoreDataMostProfits(data)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType3(data *data_batch.DataBatch) error {
	err := th.refDatasetStore.StoreDataTask3(data)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType4(data *data_batch.DataBatch) error {
	err := th.refDatasetStore.StoreDataTask4(data)
	if err != nil {
		return err
	}
	return nil
}
