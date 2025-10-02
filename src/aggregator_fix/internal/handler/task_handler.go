package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator_fix/cache"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

var log = logger.GetLogger()

type TaskHandler struct {
	taskHandlers    map[enum.TaskType]func([]byte) error
	refDatasetStore *cache.DataBatchStore
}

func NewTaskHandler(
	referenceDatasetStore *cache.DataBatchStore,
) *TaskHandler {
	th := &TaskHandler{
		refDatasetStore: referenceDatasetStore,
	}

	th.taskHandlers = map[enum.TaskType]func([]byte) error{
		enum.T1:   th.handleTaskType1,
		enum.T2_1: th.handleTaskType2_1,
		enum.T2_2: th.handleTaskType2_2,
		enum.T3:   th.handleTaskType3,
		enum.T4:   th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType enum.TaskType, payload []byte) error {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		return fmt.Errorf("unknown task type: %d", taskType)
	}
	return handler(payload)
}

func (th *TaskHandler) handleTaskType1(payload []byte) error {
	err := th.refDatasetStore.StoreDataTask1(payload)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType2_1(payload []byte) error {
	err := th.refDatasetStore.StoreDataBestSelling(payload)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType2_2(payload []byte) error {
	err := th.refDatasetStore.StoreDataMostProfits(payload)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType3(payload []byte) error {
	err := th.refDatasetStore.StoreDataTask3(payload)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType4(payload []byte) error {
	err := th.refDatasetStore.StoreDataTask4(payload)
	if err != nil {
		return err
	}
	return nil
}
