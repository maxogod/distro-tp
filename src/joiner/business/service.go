package service

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/handler"
	"google.golang.org/protobuf/proto"
)

const (
	UsersRefQueue     = "users"
	StoresRefQueue    = "stores"
	MenuItemsRefQueue = "menu_items"
	MainQueue         = 0
	BestSellingQueue  = 1
)

type TaskQueues map[enum.TaskType][]string
type AggregatorQueues map[enum.TaskType][]string
type MessageMiddlewares map[string]middleware.MessageMiddleware
type ReferenceDoneReceived map[enum.TaskType]map[string]bool

type Joiner struct {
	config               *config.Config
	referenceMiddlewares MessageMiddlewares
	dataMiddlewares      MessageMiddlewares
	taskHandler          *handler.TaskHandler
	taskQueues           TaskQueues
	mutex                sync.Mutex
	aggregatorQueues     AggregatorQueues
	aggregatorMidd       MessageMiddlewares
	refDatasetStore      *cache.ReferenceDatasetStore
	refQueueNames        []string
	refDoneReceived      ReferenceDoneReceived
	workerName           string
	controllerConnection middleware.MessageMiddleware
	finishExchange       middleware.MessageMiddleware
}

func defaultTaskQueues(config *config.Config) TaskQueues {
	return TaskQueues{
		enum.T2: {config.TransactionSumQueue, config.TransactionCountedQueue},
		enum.T3: {config.StoreTPVQueue},
		enum.T4: {config.UserTransactionsQueue},
	}
}

func defaultAggregatorQueues(config *config.Config) AggregatorQueues {
	return AggregatorQueues{
		enum.T2: {config.JoinedMostProfitsTransactionsQueue, config.JoinedBestSellingTransactionsQueue},
		enum.T3: {config.JoinedStoresTPVQueue},
		enum.T4: {config.JoinedUserTransactionsQueue},
	}
}

func defaultRequiredRefQueues() ReferenceDoneReceived {
	return ReferenceDoneReceived{
		enum.T2: {MenuItemsRefQueue: false},
		enum.T3: {StoresRefQueue: false},
		enum.T4: {UsersRefQueue: false, StoresRefQueue: false},
	}
}

func NewJoiner(config *config.Config) *Joiner {
	joiner := &Joiner{
		config:               config,
		referenceMiddlewares: make(MessageMiddlewares),
		dataMiddlewares:      make(MessageMiddlewares),
		taskQueues:           defaultTaskQueues(config),
		aggregatorQueues:     defaultAggregatorQueues(config),
		aggregatorMidd:       make(MessageMiddlewares),
		refDatasetStore:      cache.NewCacheStore(config.StorePath),
		refQueueNames:        []string{UsersRefQueue, StoresRefQueue, MenuItemsRefQueue},
		refDoneReceived:      defaultRequiredRefQueues(),
		workerName:           "joiner-" + uuid.New().String(),
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

	var err error
	j.referenceMiddlewares, err = StopConsumers(j.referenceMiddlewares)
	if err != nil {
		return err
	}

	j.dataMiddlewares, err = StopConsumers(j.dataMiddlewares)
	if err != nil {
		return err
	}

	j.aggregatorMidd, err = StopSenders(j.aggregatorMidd)
	if err != nil {
		return err
	}

	if j.controllerConnection != nil {
		if err = StopSender(j.controllerConnection); err != nil {
			return err
		}
		j.controllerConnection = nil
	}

	if j.finishExchange != nil {
		if err = StopConsumer(j.finishExchange); err != nil {
			return err
		}
		j.finishExchange = nil
	}

	return nil
}

func (j *Joiner) HandleDone(refQueueName string, taskType enum.TaskType) error {
	j.mutex.Lock()

	if _, ok := j.refDoneReceived[taskType][refQueueName]; !ok {
		return nil
	}

	j.refDoneReceived[taskType][refQueueName] = true

	j.mutex.Unlock()

	if j.allRefDatasetsLoaded(taskType) {
		for _, dataQueueName := range j.taskQueues[taskType] {
			aggQueueName := j.getAggQueueName(taskType, dataQueueName)

			aggregatorQueue, senderErr := StartSender(j.config.GatewayAddress, aggQueueName)
			if senderErr != nil {
				return senderErr
			}

			j.aggregatorMidd[aggQueueName] = aggregatorQueue

			handlerTask := j.taskHandler.HandleTask(taskType, j.isBestSellingTask(dataQueueName), aggregatorQueue)
			if err := j.startDataConsumer(handlerTask, dataQueueName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (j *Joiner) SendBatchToAggregator(dataBatch *handler.DataBatch, aggregatorMidd middleware.MessageMiddleware) error {
	dataBytes, err := proto.Marshal(dataBatch)
	if err != nil {
		return err
	}

	returnCode := aggregatorMidd.Send(dataBytes)
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

	m, err := StartAnnouncer(j.config.GatewayAddress, j.config.GatewayControllerQueue)
	if err != nil {
		return err
	}

	j.controllerConnection = m

	err = SendMessageToControllerConnection(j.controllerConnection, j.workerName, false)
	if err != nil {
		return err
	}

	exchange, err := StartDirectExchange(
		j.config.GatewayAddress,
		j.config.GatewayControllerExchange,
		j.config.FinishRoutingKey,
		j.HandleDoneDataset,
	)
	if err != nil {
		return err
	}

	j.finishExchange = exchange

	return nil
}

func (j *Joiner) allRefDatasetsLoaded(taskType enum.TaskType) bool {
	for _, received := range j.refDoneReceived[taskType] {
		if !received {
			return false
		}
	}
	return true
}

func (j *Joiner) HandleDoneDataset() error {
	j.mutex.Lock()
	defer j.mutex.Unlock()

	var err error
	j.dataMiddlewares, err = StopConsumers(j.dataMiddlewares)
	if err != nil {
		return err
	}

	j.refDoneReceived = defaultRequiredRefQueues()
	j.refDatasetStore.ResetStore()

	return SendMessageToControllerConnection(j.controllerConnection, j.workerName, true)
}

func (j *Joiner) getAggQueueName(taskType enum.TaskType, dataQueueName string) string {
	if j.isBestSellingTask(dataQueueName) {
		return j.aggregatorQueues[taskType][BestSellingQueue]
	}
	return j.aggregatorQueues[taskType][MainQueue]
}
