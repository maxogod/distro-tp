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
		config.JoinedTransactionsQueue:     enum.T2,
		config.JoinedStoresTPVQueue:        enum.T3,
		config.JoinedUserTransactionsQueue: enum.T4,
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
		config.JoinedTransactionsQueue,
		config.JoinedStoresTPVQueue,
		config.JoinedUserTransactionsQueue,
	}

	return aggregator
}

func (j *Aggregator) StartDataConsumer(dataQueueName string) error {
	taskType := j.dataQueueTaskType[dataQueueName]
	handlerTask := j.taskHandler.HandleTask(taskType)
	dataHandler := handler.NewDataHandler(handlerTask)

	m, err := StartConsumer(j.config.GatewayAddress, dataQueueName, dataHandler.HandleDataMessage)
	if err != nil {
		return err
	}
	j.dataMiddlewares[dataQueueName] = m
	return nil
}

func (j *Aggregator) Stop() error {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	var err error
	j.dataMiddlewares, err = StopConsumers(j.dataMiddlewares)
	if err != nil {
		return err
	}

	if j.gatewayConnectionQueue, err = StopSender(j.gatewayConnectionQueue); err != nil {
		return err
	}

	if j.gatewayDataQueue, err = StopSender(j.gatewayDataQueue); err != nil {
		return err
	}

	if j.finishExchange != nil {
		if err = StopConsumer(j.finishExchange); err != nil {
			return err
		}
		j.finishExchange = nil
	}

	return nil
}

func (j *Aggregator) HandleDone([]byte) error {
	// TODO: Start to send batched data to the gateway controller
	//  use SendDataBatch(dataBatch *handler.DataBatch, j.gatewayDataQueue)
	//  use StartSender() to initialize j.gatewayDataQueue
	return nil
}

func (j *Aggregator) InitService() error {
	for _, dataQueueName := range j.dataQueueNames {
		err := j.StartDataConsumer(dataQueueName)
		if err != nil {
			return err
		}
	}

	m, err := StartQueueMiddleware(j.config.GatewayAddress, j.config.GatewayControllerConnectionQueue)
	if err != nil {
		return err
	}

	j.gatewayConnectionQueue = m

	err = SendControllerConnectionMsg(j.gatewayConnectionQueue, j.workerName, false)
	if err != nil {
		return err
	}

	exchange, err := StartDirectExchange(
		j.config.GatewayAddress,
		j.config.GatewayControllerExchange,
		j.config.FinishRoutingKey,
		j.HandleDone,
	)
	if err != nil {
		return err
	}

	j.finishExchange = exchange

	return nil
}
