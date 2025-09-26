package service

import (
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/handler"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Joiner struct {
	config               *config.Config
	referenceMiddlewares map[string]middleware.MessageMiddleware
	dataMiddlewares      map[string]middleware.MessageMiddleware
	taskHandler          *handler.TaskHandler
	dataQueueNames       map[int32][]string
}

func NewJoiner(config *config.Config) *Joiner {
	joiner := &Joiner{
		config:               config,
		referenceMiddlewares: make(map[string]middleware.MessageMiddleware),
		taskHandler:          handler.NewTaskHandler(),
		dataMiddlewares:      make(map[string]middleware.MessageMiddleware),
	}

	joiner.dataQueueNames = map[int32][]string{
		2: {config.TransactionSumQueue, config.TransactionCountedQueue},
		3: {config.StoreTPVQueue},
		4: {config.UserTransactionsQueue},
	}

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

func (j *Joiner) startDataConsumer(handlerMsg func(dataBatch *handler.DataBatch), dataQueueNames []string) error {
	for _, dataQueueName := range dataQueueNames {
		dataHandler := handler.NewDataHandler(handlerMsg)

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

func (j *Joiner) HandleDone(msg *amqp.Delivery, queueName string, taskType int32) {
	if err := StopConsumer(j.referenceMiddlewares[queueName]); err != nil {
		_ = msg.Nack(false, false)
		return
	}
	delete(j.referenceMiddlewares, queueName)

	if len(j.referenceMiddlewares) == 0 {
		handlerTask := j.taskHandler.HandleTask(taskType)
		dataQueueNames := j.dataQueueNames[taskType]

		if err := j.startDataConsumer(handlerTask, dataQueueNames); err != nil {
			_ = msg.Nack(false, false)
			return
		}
	}

	_ = msg.Ack(false)
}
