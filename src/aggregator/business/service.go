package service

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/handler"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"google.golang.org/protobuf/proto"
)

type DataQueueTaskType map[string]enum.TaskType
type MessageMiddlewares map[string]middleware.MessageMiddleware
type MessageMiddlewareCreators map[string]func(string) middleware.MessageMiddleware

func defaultDataQueueTaskType(config config.Config) DataQueueTaskType {
	return DataQueueTaskType{
		config.FilteredTransactionsQueue:          enum.T1,
		config.JoinedBestSellingTransactionsQueue: enum.T2,
		config.JoinedMostProfitsTransactionsQueue: enum.T2,
		config.JoinedStoresTPVQueue:               enum.T3,
		config.JoinedUserTransactionsQueue:        enum.T4,
	}
}

func defaultDataQueueCreators(config *config.Config) MessageMiddlewareCreators {
	return MessageMiddlewareCreators{
		config.FilteredTransactionsQueue:          middleware.GetFilteredTransactionsQueue,
		config.JoinedMostProfitsTransactionsQueue: middleware.GetJoinedMostProfitsTransactionsQueue,
		config.JoinedBestSellingTransactionsQueue: middleware.GetJoinedBestSellingTransactionsQueue,
		config.JoinedStoresTPVQueue:               middleware.GetJoinedStoresTPVQueue,
		config.JoinedUserTransactionsQueue:        middleware.GetJoinedUserTransactionsQueue,
	}
}

type Aggregator struct {
	config                 *config.Config
	currentClientID        string
	dataQueueTaskType      DataQueueTaskType
	dataQueueNames         []string
	dataMiddlewares        MessageMiddlewares
	taskHandler            *handler.TaskHandler
	mutex                  sync.Mutex
	gatewayDataQueue       middleware.MessageMiddleware
	gatewayConnectionQueue middleware.MessageMiddleware
	refDatasetStore        *cache.DataBatchStore
	workerName             string
	finishExchange         middleware.MessageMiddleware
	dataQueueCreators      MessageMiddlewareCreators
}

func NewAggregator(config *config.Config) *Aggregator {
	aggregator := &Aggregator{
		config:            config,
		dataQueueTaskType: defaultDataQueueTaskType(*config),
		dataMiddlewares:   make(MessageMiddlewares),
		refDatasetStore:   cache.NewCacheStore(config.StorePath),
		dataQueueCreators: defaultDataQueueCreators(config),
		workerName:        "aggregator_" + uuid.New().String(),
	}

	aggregator.taskHandler = handler.NewTaskHandler(aggregator.refDatasetStore)
	aggregator.dataQueueNames = []string{
		config.JoinedBestSellingTransactionsQueue,
		config.JoinedMostProfitsTransactionsQueue,
		config.JoinedStoresTPVQueue,
		config.JoinedUserTransactionsQueue,
		config.FilteredTransactionsQueue,
	}

	return aggregator
}

func (a *Aggregator) StartDataConsumer(dataQueueName string) error {
	taskType := a.dataQueueTaskType[dataQueueName]
	handlerTask := a.taskHandler.HandleTask(taskType, a.isBestSellingTask(dataQueueName))
	dataHandler := handler.NewDataHandler(handlerTask)
	a.dataMiddlewares[dataQueueName] = a.dataQueueCreators[dataQueueName](a.config.GatewayAddress)
	return StartConsumer(a.dataMiddlewares[dataQueueName], dataHandler.HandleDataMessage)
}

func (a *Aggregator) isBestSellingTask(dataQueueName string) bool {
	return dataQueueName == a.config.JoinedBestSellingTransactionsQueue
}

func (a *Aggregator) Stop() error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	var err error
	a.dataMiddlewares, err = StopConsumers(a.dataMiddlewares)
	if err != nil {
		return err
	}

	if a.gatewayConnectionQueue, err = StopSender(a.gatewayConnectionQueue); err != nil {
		return err
	}

	if a.gatewayDataQueue, err = StopSender(a.gatewayDataQueue); err != nil {
		return err
	}

	if a.finishExchange != nil {
		if err = StopConsumer(a.finishExchange); err != nil {
			return err
		}
		a.finishExchange = nil
	}

	a.refDatasetStore.ResetStore()

	return nil
}

func (a *Aggregator) HandleDone(msgBatch []byte) error {
	log.Debugln("Received done message from gateway")
	batch := &data_batch.DataBatch{}
	if err := proto.Unmarshal(msgBatch, batch); err != nil {
		return err
	}
	a.currentClientID = batch.ClientId

	a.gatewayDataQueue = middleware.GetProcessedDataQueue(a.config.GatewayAddress)

	var err error
	switch enum.TaskType(batch.TaskType) {
	case enum.T1:
		err = a.AggregateDataTask1()
	case enum.T2:
		err = a.AggregateDataTask2()
	case enum.T3:
		err = a.AggregateDataTask3()
	case enum.T4:
		err = a.AggregateDataTask4()
	default:
		err = fmt.Errorf("unknown task type: %v", batch.TaskType)
	}
	if err != nil {
		return err
	}

	return SendControllerConnectionMsg(a.gatewayConnectionQueue, a.workerName, a.currentClientID, true)
}

func (a *Aggregator) InitService() error {
	for _, dataQueueName := range a.dataQueueNames {
		log.Debugln("Starting data consumer for queue: %s", dataQueueName)
		err := a.StartDataConsumer(dataQueueName)
		if err != nil {
			return err
		}
	}

	a.gatewayConnectionQueue = middleware.GetNodeConnectionsQueue(a.config.GatewayAddress)

	err := SendControllerConnectionMsg(a.gatewayConnectionQueue, a.workerName, a.currentClientID, false)
	if err != nil {
		return err
	}

	err = a.StartDirectExchange()

	return nil
}

func (a *Aggregator) StartDirectExchange() error {
	a.finishExchange = middleware.GetFinishExchange(a.config.GatewayAddress, enum.Aggregator)

	err := StartDirectExchange(
		a.finishExchange,
		a.HandleDone,
	)
	if err != nil {
		return err
	}

	return nil
}
