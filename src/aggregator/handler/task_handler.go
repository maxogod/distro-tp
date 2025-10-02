package handler

import (
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

const (
	mainHandler        = 0
	bestSellingHandler = 0
	mostProfitsHandler = 1
)

type TaskHandlers map[enum.TaskType][]HandleTask

type TaskHandler struct {
	taskHandlers    TaskHandlers
	refDatasetStore *cache.DataBatchStore
}

func NewTaskHandler(referenceDatasetStore *cache.DataBatchStore) *TaskHandler {
	th := &TaskHandler{
		refDatasetStore: referenceDatasetStore,
	}

	th.taskHandlers = TaskHandlers{
		enum.T1: []HandleTask{th.handleTaskType1},
		enum.T2: []HandleTask{th.handleBestSelling, th.handleMostProfits},
		enum.T3: []HandleTask{th.handleTaskType3},
		enum.T4: []HandleTask{th.handleTaskType4},
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType enum.TaskType, isBestSellingTask bool) HandleTask {
	taskHandler := th.taskHandlers[taskType]
	if taskType != enum.T2 {
		return taskHandler[mainHandler]
	}

	taskHandlerT2 := mostProfitsHandler
	if isBestSellingTask {
		taskHandlerT2 = bestSellingHandler
	}

	return taskHandler[taskHandlerT2]
}

func (th *TaskHandler) handleTaskType1(dataBatch *data_batch.DataBatch) error {
	err := th.refDatasetStore.StoreDataTask1(dataBatch)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleBestSelling(dataBatch *data_batch.DataBatch) error {
	err := th.refDatasetStore.StoreDataBestSelling(dataBatch)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleMostProfits(dataBatch *data_batch.DataBatch) error {
	err := th.refDatasetStore.StoreDataMostProfits(dataBatch)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType3(dataBatch *data_batch.DataBatch) error {
	err := th.refDatasetStore.StoreDataTask3(dataBatch)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType4(dataBatch *data_batch.DataBatch) error {
	err := th.refDatasetStore.StoreDataTask4(dataBatch)
	if err != nil {
		return err
	}
	return nil
}
