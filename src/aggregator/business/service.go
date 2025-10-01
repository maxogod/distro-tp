package service

import (
	"sync"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/handler"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

type DataQueueTaskType map[string]enum.TaskType
type MessageMiddlewares map[string]middleware.MessageMiddleware

func defaultDataQueueTaskType(config config.Config) DataQueueTaskType {
	return DataQueueTaskType{
		config.JoinedBestSellingTransactionsQueue: enum.T2,
		config.JoinedMostProfitsTransactionsQueue: enum.T2,
		config.JoinedStoresTPVQueue:               enum.T3,
		config.JoinedUserTransactionsQueue:        enum.T4,
	}
}

type Aggregator struct {
	config                 *config.Config
	dataQueueTaskType      DataQueueTaskType
	dataQueueNames         []string
	dataMiddlewares        MessageMiddlewares
	taskHandler            *handler.TaskHandler
	mutex                  sync.Mutex
	gatewayDataQueue       middleware.MessageMiddleware
	gatewayConnectionQueue middleware.MessageMiddleware
	refDatasetStore        *cache.ReferenceDatasetStore
	workerName             string
	finishExchange         middleware.MessageMiddleware
}

func NewAggregator(config *config.Config) *Aggregator {
	aggregator := &Aggregator{
		config:            config,
		dataQueueTaskType: defaultDataQueueTaskType(*config),
		dataMiddlewares:   make(MessageMiddlewares),
		refDatasetStore:   cache.NewCacheStore(config.StorePath),
		workerName:        "aggregator" + uuid.New().String(),
	}

	aggregator.taskHandler = handler.NewTaskHandler(aggregator.refDatasetStore)
	aggregator.dataQueueNames = []string{
		config.JoinedBestSellingTransactionsQueue,
		config.JoinedMostProfitsTransactionsQueue,
		config.JoinedStoresTPVQueue,
		config.JoinedUserTransactionsQueue,
	}

	return aggregator
}

func (a *Aggregator) StartDataConsumer(dataQueueName string) error {
	taskType := a.dataQueueTaskType[dataQueueName]
	handlerTask := a.taskHandler.HandleTask(taskType, a.isBestSellingTask(dataQueueName))
	dataHandler := handler.NewDataHandler(handlerTask)

	m, err := StartConsumer(a.config.GatewayAddress, dataQueueName, dataHandler.HandleDataMessage)
	if err != nil {
		return err
	}
	a.dataMiddlewares[dataQueueName] = m
	return nil
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

	return nil
}

func (a *Aggregator) HandleDone([]byte) error {
	// TODO: Start to send batched data to the gateway controller
	//  use SendDataBatch(dataBatch *handler.DataBatch, a.gatewayDataQueue)
	//  use StartSender() to initialize a.gatewayDataQueue
	return nil
}

func (a *Aggregator) InitService() error {
	for _, dataQueueName := range a.dataQueueNames {
		err := a.StartDataConsumer(dataQueueName)
		if err != nil {
			return err
		}
	}

	m, err := StartQueueMiddleware(a.config.GatewayAddress, a.config.GatewayControllerConnectionQueue)
	if err != nil {
		return err
	}

	a.gatewayConnectionQueue = m

	err = SendControllerConnectionMsg(a.gatewayConnectionQueue, a.workerName, false)
	if err != nil {
		return err
	}

	err = a.StartDirectExchange()

	return nil
}

func (a *Aggregator) StartDirectExchange() error {
	exchange, err := StartDirectExchange(
		a.config.GatewayAddress,
		a.config.GatewayControllerExchange,
		a.config.FinishRoutingKey,
		a.HandleDone,
	)
	if err != nil {
		return err
	}

	a.finishExchange = exchange
	return nil
}
