package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"google.golang.org/protobuf/proto"
)

type TaskHandlers map[models.TaskType]HandleTask
type SendBatchToAggregator func(dataBatch *protocol.DataBatch) error

type TaskHandler struct {
	taskHandlers          TaskHandlers
	sendBatchToAggregator SendBatchToAggregator
}

func NewTaskHandler(sender SendBatchToAggregator) *TaskHandler {
	th := &TaskHandler{
		sendBatchToAggregator: sender,
	}

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

func (th *TaskHandler) handleTaskType2(dataBatch *protocol.DataBatch, refDatasetDir string) error {
	return nil
}

func (th *TaskHandler) handleTaskType3(dataBatch *protocol.DataBatch, refDatasetDir string) error {
	pathStoresDataset := fmt.Sprintf("%s/stores.csv", refDatasetDir)
	storesMap, loadErr := cache.LoadStores(pathStoresDataset)
	if loadErr != nil {
		return loadErr
	}

	var storeTPVsContainer protocol.StoresTPV
	err := proto.Unmarshal(dataBatch.Payload, &storeTPVsContainer)
	if err != nil {
		return err
	}

	storeTPVs := storeTPVsContainer.Items

	joined := make([]*protocol.JoinStoreTPV, 0)
	for _, entry := range storeTPVs {
		if store, ok := storesMap[entry.StoreId]; ok {
			joined = append(joined, &protocol.JoinStoreTPV{
				YearHalfCreatedAt: entry.YearHalfCreatedAt,
				StoreName:         store.StoreName,
				Tpv:               entry.Tpv,
			})
		}
	}

	joinedDataBatch, err := cache.CreateDataBatchFromJoined(dataBatch.TaskType, joined)
	if err != nil {
		return err
	}

	err = th.sendBatchToAggregator(joinedDataBatch)
	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType4(dataBatch *protocol.DataBatch, refDatasetDir string) error {
	return nil
}
