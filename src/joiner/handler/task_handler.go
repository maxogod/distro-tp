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

type MenuItemsMap map[int32]*protocol.MenuItem

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

func (th *TaskHandler) handleTaskType2(dataBatch *protocol.DataBatch, refDatasetDir string, isBestSellingTask bool) error {
	pathMenuItemsDataset := fmt.Sprintf("%s/menu_items.pb", refDatasetDir)
	menuItemsMap, loadErr := cache.LoadMenuItems(pathMenuItemsDataset)
	if loadErr != nil {
		return loadErr
	}

	if isBestSellingTask {
		return th.handleTaskType2BestSellingProducts(dataBatch, menuItemsMap)
	}
	return th.handleTaskType2MostProfitsProducts(dataBatch, menuItemsMap)
}

func (th *TaskHandler) handleTaskType2BestSellingProducts(dataBatch *protocol.DataBatch, menuItemsMap map[int32]*protocol.MenuItem) error {

	var bestSellingProductsBatch protocol.BestSellingProductsBatch
	err := proto.Unmarshal(dataBatch.Payload, &bestSellingProductsBatch)
	if err != nil {
		return err
	}

	bestSellingProducts := bestSellingProductsBatch.Items

	joined := make([]*protocol.JoinBestSellingProducts, 0)
	for _, entry := range bestSellingProducts {
		if item, ok := menuItemsMap[entry.ItemId]; ok {
			joined = append(joined, &protocol.JoinBestSellingProducts{
				YearMonthCreatedAt: entry.YearMonthCreatedAt,
				ItemName:           item.ItemName,
				SellingsQty:        entry.SellingsQty,
			})
		}
	}

	joinedDataBatch, err := cache.CreateBestSellingBatch(dataBatch.TaskType, joined)
	if err != nil {
		return err
	}

	err = th.sendBatchToAggregator(joinedDataBatch)
	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType2MostProfitsProducts(dataBatch *protocol.DataBatch, menuItemsMap MenuItemsMap) error {
	var mostProfitsProductsBatch protocol.MostProfitsProductsBatch
	err := proto.Unmarshal(dataBatch.Payload, &mostProfitsProductsBatch)
	if err != nil {
		return err
	}

	mostProfitsProducts := mostProfitsProductsBatch.Items

	joined := make([]*protocol.JoinMostProfitsProducts, 0)
	for _, entry := range mostProfitsProducts {
		if item, ok := menuItemsMap[entry.ItemId]; ok {
			joined = append(joined, &protocol.JoinMostProfitsProducts{
				YearMonthCreatedAt: entry.YearMonthCreatedAt,
				ItemName:           item.ItemName,
				ProfitSum:          entry.ProfitSum,
			})
		}
	}

	joinedDataBatch, err := cache.CreateMostProfitsBatch(dataBatch.TaskType, joined)
	if err != nil {
		return err
	}

	err = th.sendBatchToAggregator(joinedDataBatch)
	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType3(dataBatch *protocol.DataBatch, refDatasetDir string, isBestSellingTask bool) error {
	pathStoresDataset := fmt.Sprintf("%s/stores.pb", refDatasetDir)
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

	joinedDataBatch, err := cache.CreateJoinStoreTPVBatch(dataBatch.TaskType, joined)
	if err != nil {
		return err
	}

	err = th.sendBatchToAggregator(joinedDataBatch)
	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType4(dataBatch *protocol.DataBatch, refDatasetDir string, isBestSellingTask bool) error {
	return nil
}
