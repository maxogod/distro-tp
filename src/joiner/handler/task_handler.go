package handler

import (
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"google.golang.org/protobuf/proto"
)

const (
	mainHandler        = 0
	bestSellingHandler = 0
	mostProfitsHandler = 1
)

type TaskHandlers map[enum.TaskType][]HandleTask
type TaskHandlerAggregators map[KeyAggregator]middleware.MessageMiddleware
type SendBatchToAggregator func(dataBatch *data_batch.DataBatch, aggregator middleware.MessageMiddleware) error

type KeyAggregator struct {
	TaskType          enum.TaskType
	IsBestSellingTask bool
}

type TaskHandler struct {
	taskHandlers          TaskHandlers
	sendBatchToAggregator SendBatchToAggregator
	refDatasetStore       *cache.ReferenceDatasetStore
	taskHadlerAggregator  TaskHandlerAggregators
}

func NewTaskHandler(sender SendBatchToAggregator, referenceDatasetStore *cache.ReferenceDatasetStore) *TaskHandler {
	th := &TaskHandler{
		sendBatchToAggregator: sender,
		refDatasetStore:       referenceDatasetStore,
		taskHadlerAggregator:  make(TaskHandlerAggregators),
	}

	th.taskHandlers = TaskHandlers{
		enum.T2: []HandleTask{th.handleBestSellingProducts, th.handleMostProfitsProducts},
		enum.T3: []HandleTask{th.handleTaskType3},
		enum.T4: []HandleTask{th.handleTaskType4},
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType enum.TaskType, isBestSellingTask bool, aggQueue middleware.MessageMiddleware) HandleTask {
	taskHandler := th.taskHandlers[taskType]
	if taskType != enum.T2 {
		th.taskHadlerAggregator[KeyAggregator{TaskType: taskType, IsBestSellingTask: false}] = aggQueue
		return taskHandler[mainHandler]
	}

	taskHandlerT2 := mostProfitsHandler
	if isBestSellingTask {
		taskHandlerT2 = bestSellingHandler
	}

	th.taskHadlerAggregator[KeyAggregator{TaskType: taskType, IsBestSellingTask: isBestSellingTask}] = aggQueue

	return taskHandler[taskHandlerT2]
}

func (th *TaskHandler) handleBestSellingProducts(dataBatch *data_batch.DataBatch) error {
	menuItemsMap, loadErr := th.refDatasetStore.LoadMenuItems()
	if loadErr != nil {
		return loadErr
	}

	var bestSellingProductsBatch reduced.BestSellingProductsBatch
	err := proto.Unmarshal(dataBatch.Payload, &bestSellingProductsBatch)
	if err != nil {
		return err
	}

	bestSellingProducts := bestSellingProductsBatch.Items

	joinedData := make([]*joined.JoinBestSellingProducts, 0)
	for _, entry := range bestSellingProducts {
		if item, ok := menuItemsMap[entry.ItemId]; ok {
			joinedData = append(joinedData, &joined.JoinBestSellingProducts{
				YearMonthCreatedAt: entry.YearMonthCreatedAt,
				ItemName:           item.ItemName,
				SellingsQty:        entry.SellingsQty,
			})
		}
	}

	aggregator := th.taskHadlerAggregator[KeyAggregator{TaskType: enum.TaskType(dataBatch.TaskType), IsBestSellingTask: true}]

	return sendJoinedData(dataBatch, joinedData, cache.CreateBestSellingBatch, th.sendBatchToAggregator, aggregator)
}

func (th *TaskHandler) handleMostProfitsProducts(dataBatch *data_batch.DataBatch) error {
	menuItemsMap, loadErr := th.refDatasetStore.LoadMenuItems()
	if loadErr != nil {
		return loadErr
	}

	var mostProfitsProductsBatch reduced.MostProfitsProductsBatch
	err := proto.Unmarshal(dataBatch.Payload, &mostProfitsProductsBatch)
	if err != nil {
		return err
	}

	mostProfitsProducts := mostProfitsProductsBatch.Items

	joinedData := make([]*joined.JoinMostProfitsProducts, 0)
	for _, entry := range mostProfitsProducts {
		if item, ok := menuItemsMap[entry.ItemId]; ok {
			joinedData = append(joinedData, &joined.JoinMostProfitsProducts{
				YearMonthCreatedAt: entry.YearMonthCreatedAt,
				ItemName:           item.ItemName,
				ProfitSum:          entry.ProfitSum,
			})
		}
	}

	aggregator := th.taskHadlerAggregator[KeyAggregator{TaskType: enum.TaskType(dataBatch.TaskType), IsBestSellingTask: false}]

	return sendJoinedData(dataBatch, joinedData, cache.CreateMostProfitsBatch, th.sendBatchToAggregator, aggregator)
}

func (th *TaskHandler) handleTaskType3(dataBatch *data_batch.DataBatch) error {
	storesMap, loadErr := th.refDatasetStore.LoadStores()
	if loadErr != nil {
		return loadErr
	}

	var storeTPVsContainer reduced.StoresTPV
	err := proto.Unmarshal(dataBatch.Payload, &storeTPVsContainer)
	if err != nil {
		return err
	}

	storeTPVs := storeTPVsContainer.Items

	joinedData := make([]*joined.JoinStoreTPV, 0)
	for _, entry := range storeTPVs {
		if store, ok := storesMap[entry.StoreId]; ok {
			joinedData = append(joinedData, &joined.JoinStoreTPV{
				YearHalfCreatedAt: entry.YearHalfCreatedAt,
				StoreName:         store.StoreName,
				Tpv:               entry.Tpv,
			})
		}
	}

	aggregator := th.taskHadlerAggregator[KeyAggregator{TaskType: enum.TaskType(dataBatch.TaskType), IsBestSellingTask: false}]

	return sendJoinedData(dataBatch, joinedData, cache.CreateJoinStoreTPVBatch, th.sendBatchToAggregator, aggregator)
}

func (th *TaskHandler) handleTaskType4(dataBatch *data_batch.DataBatch) error {
	storesMap, loadErr := th.refDatasetStore.LoadStores()
	if loadErr != nil {
		return loadErr
	}

	var mostPurchasesUserBatch reduced.MostPurchasesUserBatch
	err := proto.Unmarshal(dataBatch.Payload, &mostPurchasesUserBatch)
	if err != nil {
		return err
	}

	mostPurchasesUsers := mostPurchasesUserBatch.Users
	userIDs := make([]int, 0, len(mostPurchasesUsers))
	for _, user := range mostPurchasesUsers {
		userIDs = append(userIDs, int(user.UserId))
	}

	usersMap, loadErr := th.refDatasetStore.LoadUsers(userIDs)
	if loadErr != nil {
		return loadErr
	}

	joinedData := make([]*joined.JoinMostPurchasesUser, 0)
	for _, entry := range mostPurchasesUsers {
		store := storesMap[entry.StoreId]
		user := usersMap[entry.UserId]

		joinedData = append(joinedData, &joined.JoinMostPurchasesUser{
			StoreName:     store.StoreName,
			UserBirthdate: user.Birthdate,
			PurchasesQty:  entry.PurchasesQty,
		})
	}

	aggregator := th.taskHadlerAggregator[KeyAggregator{TaskType: enum.TaskType(dataBatch.TaskType), IsBestSellingTask: false}]

	return sendJoinedData(dataBatch, joinedData, cache.CreateMostPurchasesUserBatch, th.sendBatchToAggregator, aggregator)
}

func sendJoinedData[T any](
	dataBatch *data_batch.DataBatch,
	joined []*T,
	createBatch func(taskType enum.TaskType, items []*T) (*data_batch.DataBatch, error),
	sendBatchToAggregator SendBatchToAggregator,
	aggregator middleware.MessageMiddleware,
) error {
	joinedDataBatch, err := createBatch(enum.TaskType(dataBatch.TaskType), joined)
	if err != nil {
		return err
	}
	return sendBatchToAggregator(joinedDataBatch, aggregator)
}
