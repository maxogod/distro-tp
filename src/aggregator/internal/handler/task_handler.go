package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

var log = logger.GetLogger()

type TaskHandler struct {
	taskHandlers    map[enum.TaskType]func(*data_batch.DataBatch) error
	refDatasetStore map[enum.TaskType]*cache.DataBatchStore
	storePath       string
}

func NewTaskHandler(storePath string) *TaskHandler {
	th := &TaskHandler{
		refDatasetStore: make(map[enum.TaskType]*cache.DataBatchStore),
		storePath:       storePath,
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
	if _, exists := th.refDatasetStore[enum.T1]; !exists {
		th.refDatasetStore[enum.T1] = cache.NewCacheStore()
	}

	err := th.refDatasetStore[enum.T1].StoreData(data)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType2_1(data *data_batch.DataBatch) error {
	if _, exists := th.refDatasetStore[enum.T2_1]; !exists {
		th.refDatasetStore[enum.T2_1] = cache.NewCacheStore()
	}

	err := th.refDatasetStore[enum.T2_1].StoreData(data)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType2_2(data *data_batch.DataBatch) error {
	if _, exists := th.refDatasetStore[enum.T2_2]; !exists {
		th.refDatasetStore[enum.T2_2] = cache.NewCacheStore()
	}

	err := th.refDatasetStore[enum.T2_2].StoreData(data)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType3(data *data_batch.DataBatch) error {
	if _, exists := th.refDatasetStore[enum.T3]; !exists {
		th.refDatasetStore[enum.T3] = cache.NewCacheStore()
	}

	err := th.refDatasetStore[enum.T3].StoreData(data)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) handleTaskType4(data *data_batch.DataBatch) error {
	if _, exists := th.refDatasetStore[enum.T4]; !exists {
		th.refDatasetStore[enum.T4] = cache.NewCacheStore()
	}

	err := th.refDatasetStore[enum.T4].StoreData(data)
	if err != nil {
		return err
	}
	return nil
}

func (th *TaskHandler) ResetStore() error {
	for _, store := range th.refDatasetStore {
		if err := store.ResetStore(); err != nil {
			log.Errorf("Error resetting store: %v", err)
			return err
		}
	}
	return nil
}

func (th *TaskHandler) HandleDataTask1() (business.MapTransactions, error) {
	return business.AggregateDataTask1(th.refDatasetStore[enum.T1])
}

func (th *TaskHandler) HandleBestSellingData() (business.MapJoinBestSelling, error) {
	return business.AggregateBestSellingData(th.refDatasetStore[enum.T2_1])
}

func (th *TaskHandler) HandleMostProfitsData() (business.MapJoinMostProfits, error) {
	return business.AggregateMostProfitsData(th.refDatasetStore[enum.T2_2])
}

func (th *TaskHandler) HandleDataTask3() (business.MapJoinStoreTPV, error) {
	return business.AggregateDataTask3(th.refDatasetStore[enum.T3])
}

func (th *TaskHandler) HandleDataTask4() (business.MapJoinMostPurchasesUser, error) {
	return business.AggregateDataTask4(th.refDatasetStore[enum.T4])
}
