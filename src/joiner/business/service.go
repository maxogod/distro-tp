package service

import (
	"fmt"
	"sync"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/handler"
	"google.golang.org/protobuf/proto"
)

type TaskQueues map[models.TaskType][]string
type AggregatorQueues map[models.TaskType]string
type MessageMiddlewares map[string]middleware.MessageMiddleware

type Joiner struct {
	config               *config.Config
	referenceMiddlewares MessageMiddlewares
	dataMiddlewares      MessageMiddlewares
	taskHandler          *handler.TaskHandler
	taskQueues           TaskQueues
	mutex                sync.Mutex
	aggregatorQueues     AggregatorQueues
	aggregatorQueue      middleware.MessageMiddleware
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

func NewJoiner(config *config.Config) *Joiner {
	joiner := &Joiner{
		config:               config,
		referenceMiddlewares: make(MessageMiddlewares),
		dataMiddlewares:      make(MessageMiddlewares),
		taskQueues:           defaultTaskQueues(config),
		aggregatorQueues:     defaultAggregatorQueues(config),
	}

	joiner.taskHandler = handler.NewTaskHandler(joiner.SendBatchToAggregator)

	return joiner
}

func (j *Joiner) StartRefConsumer(referenceDatasetQueue string) error {
	referenceHandler := handler.NewReferenceHandler(j.HandleDone, referenceDatasetQueue, j.config.StorePath)

	m, err := StartConsumer(j.config.GatewayAddress, referenceDatasetQueue, referenceHandler.HandleReferenceQueueMessage)
	if err != nil {
		return err
	}

	j.referenceMiddlewares[referenceDatasetQueue] = m
	return nil
}

func (j *Joiner) startDataConsumer(handlerTask handler.HandleTask, dataQueueNames []string) error {
	for _, dataQueueName := range dataQueueNames {
		isBestSellingTask := j.isBestSellingTask(dataQueueName)
		dataHandler := handler.NewDataHandler(handlerTask, j.config.StorePath, isBestSellingTask)

		m, err := StartConsumer(j.config.GatewayAddress, dataQueueName, dataHandler.HandleDataMessage)
		if err != nil {
			return err
		}
		j.dataMiddlewares[dataQueueName] = m
	}
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
	return nil
}

func (j *Joiner) HandleDone(queueName string, taskType models.TaskType) error {
	j.mutex.Lock()

	if referenceMiddleware, ok := j.referenceMiddlewares[queueName]; ok {
		if err := StopConsumer(referenceMiddleware); err != nil {
			return err
		}
		delete(j.referenceMiddlewares, queueName)
	}

	var handlerTask handler.HandleTask
	var dataQueueNames []string
	if len(j.referenceMiddlewares) == 0 {
		handlerTask = j.taskHandler.HandleTask(taskType)
		dataQueueNames = j.taskQueues[taskType]
	}

	j.mutex.Unlock()

	if handlerTask != nil {
		aggQueueName := j.aggregatorQueues[taskType]

		// TODO: Hacer StopSender() en el handle del DoneMsg para los DataBatches
		aggregatorQueue, senderErr := StartSender(j.config.GatewayAddress, aggQueueName)
		if senderErr != nil {
			return senderErr
		}

		j.aggregatorQueue = aggregatorQueue

		if err := j.startDataConsumer(handlerTask, dataQueueNames); err != nil {
			return err
		}
	}

	return nil
}

func (j *Joiner) SendBatchToAggregator(dataBatch *handler.DataBatch) error {
	dataBytes, err := proto.Marshal(dataBatch)
	if err != nil {
		return err
	}

	returnCode := j.aggregatorQueue.Send(dataBytes)
	if returnCode != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to send result: %d", returnCode)
	}

	return nil
}

func (j *Joiner) isBestSellingTask(dataQueueName string) bool {
	return dataQueueName == j.config.TransactionCountedQueue
}
