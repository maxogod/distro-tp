package service

import (
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/handler"
)

type TaskQueues map[models.TaskType][]string
type MessageMiddlewares map[string]middleware.MessageMiddleware

type Joiner struct {
	config               *config.Config
	referenceMiddlewares MessageMiddlewares
	dataMiddlewares      MessageMiddlewares
	taskHandler          *handler.TaskHandler
	taskQueues           TaskQueues
}

func defaultTaskQueues(config *config.Config) TaskQueues {
	return TaskQueues{
		models.T2: {config.TransactionSumQueue, config.TransactionCountedQueue},
		models.T3: {config.StoreTPVQueue},
		models.T4: {config.UserTransactionsQueue},
	}
}

func NewJoiner(config *config.Config) *Joiner {
	return &Joiner{
		config:               config,
		referenceMiddlewares: make(MessageMiddlewares),
		taskHandler:          handler.NewTaskHandler(),
		dataMiddlewares:      make(MessageMiddlewares),
		taskQueues:           defaultTaskQueues(config),
	}
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
		dataHandler := handler.NewDataHandler(handlerTask)

		m, err := StartConsumer(j.config.GatewayAddress, dataQueueName, dataHandler.HandleDataMessage)
		if err != nil {
			return err
		}
		j.dataMiddlewares[dataQueueName] = m
	}
	return nil
}

func (j *Joiner) Stop() error {
	for _, refMiddleware := range j.referenceMiddlewares {
		if err := StopConsumer(refMiddleware); err != nil {
			return err
		}
	}
	return nil
}

func (j *Joiner) HandleDone(queueName string, taskType models.TaskType) error {
	if err := StopConsumer(j.referenceMiddlewares[queueName]); err != nil {
		return err
	}
	delete(j.referenceMiddlewares, queueName)

	if len(j.referenceMiddlewares) == 0 {
		handlerTask := j.taskHandler.HandleTask(taskType)
		dataQueueNames := j.taskQueues[models.TaskType(taskType)]

		if err := j.startDataConsumer(handlerTask, dataQueueNames); err != nil {
			return err
		}
	}

	return nil
}
