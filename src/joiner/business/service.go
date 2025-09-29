package service

import (
	"fmt"
	"sync"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/handler"
	"google.golang.org/protobuf/proto"
)

const (
	UsersRefQueue     = "users"
	StoresRefQueue    = "stores"
	MenuItemsRefQueue = "menu_items"
)

type TaskQueues map[models.TaskType][]string
type AggregatorQueues map[models.TaskType]string
type MessageMiddlewares map[string]middleware.MessageMiddleware
type ReferenceDoneReceived map[models.TaskType]map[string]bool

type Joiner struct {
	config               *config.Config
	referenceMiddlewares MessageMiddlewares
	dataMiddlewares      MessageMiddlewares
	taskHandler          *handler.TaskHandler
	taskQueues           TaskQueues
	mutex                sync.Mutex
	aggregatorQueues     AggregatorQueues
	aggregatorMidd       middleware.MessageMiddleware
	refDatasetStore      *cache.ReferenceDatasetStore
	refQueueNames        []string
	refDoneReceived      ReferenceDoneReceived
}

func defaultTaskQueues(config *config.Config) TaskQueues {
	return TaskQueues{
		models.T2: {config.TransactionSumQueue, config.TransactionCountedQueue},
		models.T3: {config.StoreTPVQueue},
		models.T4: {config.UserTransactionsQueue},
	}
}

func defaultAggregatorQueues(config *config.Config) AggregatorQueues {
	return AggregatorQueues{
		models.T2: config.JoinedTransactionsQueue,
		models.T3: config.JoinedStoresTPVQueue,
		models.T4: config.JoinedUserTransactionsQueue,
	}
}

func defaultRequiredRefQueues() ReferenceDoneReceived {
	return ReferenceDoneReceived{
		models.T2: {MenuItemsRefQueue: false},
		models.T3: {StoresRefQueue: false},
		models.T4: {UsersRefQueue: false, StoresRefQueue: false},
	}
}

func NewJoiner(config *config.Config) *Joiner {
	joiner := &Joiner{
		config:               config,
		referenceMiddlewares: make(MessageMiddlewares),
		dataMiddlewares:      make(MessageMiddlewares),
		taskQueues:           defaultTaskQueues(config),
		aggregatorQueues:     defaultAggregatorQueues(config),
		refDatasetStore:      cache.NewCacheStore(config.StorePath),
		refQueueNames:        []string{UsersRefQueue, StoresRefQueue, MenuItemsRefQueue},
		refDoneReceived:      defaultRequiredRefQueues(),
	}

	joiner.taskHandler = handler.NewTaskHandler(joiner.SendBatchToAggregator, joiner.refDatasetStore)

	return joiner
}

func (j *Joiner) StartRefConsumer(referenceDatasetQueue string) error {
	referenceHandler := handler.NewReferenceHandler(j.HandleDone, referenceDatasetQueue, j.refDatasetStore)

	m, err := StartConsumer(j.config.GatewayAddress, referenceDatasetQueue, referenceHandler.HandleReferenceQueueMessage)
	if err != nil {
		return err
	}

	j.referenceMiddlewares[referenceDatasetQueue] = m
	return nil
}

func (j *Joiner) startDataConsumer(handlerTask handler.HandleTask, dataQueueName string) error {
	dataHandler := handler.NewDataHandler(handlerTask)

	m, err := StartConsumer(j.config.GatewayAddress, dataQueueName, dataHandler.HandleDataMessage)
	if err != nil {
		return err
	}
	j.dataMiddlewares[dataQueueName] = m
	return nil
}

func (j *Joiner) Stop() error {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	for _, refMiddleware := range j.referenceMiddlewares {
		if err := StopConsumer(refMiddleware); err != nil {
			return err
		}
	}
	j.referenceMiddlewares = make(MessageMiddlewares)

	for _, dataMiddleware := range j.dataMiddlewares {
		if err := StopConsumer(dataMiddleware); err != nil {
			return err
		}
	}
	j.dataMiddlewares = make(MessageMiddlewares)

	if j.aggregatorMidd != nil {
		if err := StopSender(j.aggregatorMidd); err != nil {
			return err
		}
		j.aggregatorMidd = nil
	}
	return nil
}

func (j *Joiner) HandleDone(refQueueName string, taskType models.TaskType) error {
	j.mutex.Lock()

	if _, ok := j.refDoneReceived[taskType][refQueueName]; !ok {
		return nil
	}

	if referenceMiddleware, ok := j.referenceMiddlewares[refQueueName]; ok {
		if err := StopConsumer(referenceMiddleware); err != nil {
			return err
		}
		delete(j.referenceMiddlewares, refQueueName)
	}

	j.refDoneReceived[taskType][refQueueName] = true

	j.mutex.Unlock()

	if j.allRefDatasetsLoaded(taskType) {
		aggQueueName := j.aggregatorQueues[taskType]

		aggregatorQueue, senderErr := StartSender(j.config.GatewayAddress, aggQueueName)
		if senderErr != nil {
			return senderErr
		}

		j.aggregatorMidd = aggregatorQueue

		for _, dataQueueName := range j.taskQueues[taskType] {
			handlerTask := j.taskHandler.HandleTask(taskType, j.isBestSellingTask(dataQueueName))
			if err := j.startDataConsumer(handlerTask, dataQueueName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (j *Joiner) SendBatchToAggregator(dataBatch *handler.DataBatch) error {
	dataBytes, err := proto.Marshal(dataBatch)
	if err != nil {
		return err
	}

	returnCode := j.aggregatorMidd.Send(dataBytes)
	if returnCode != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to send result: %d", returnCode)
	}

	return nil
}

func (j *Joiner) isBestSellingTask(dataQueueName string) bool {
	return dataQueueName == j.config.TransactionCountedQueue
}

func (j *Joiner) InitService() error {
	for _, refQueueName := range j.refQueueNames {
		err := j.StartRefConsumer(refQueueName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (j *Joiner) allRefDatasetsLoaded(taskType models.TaskType) bool {
	for _, received := range j.refDoneReceived[taskType] {
		if !received {
			return false
		}
	}
	return true
}
