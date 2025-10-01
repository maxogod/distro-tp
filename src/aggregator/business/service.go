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
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"google.golang.org/protobuf/proto"
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
		workerName:        "aggregator-" + uuid.New().String(),
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

func (a *Aggregator) HandleDone(msgBatch []byte) error {
	batch := &data_batch.DataBatch{}
	if err := proto.Unmarshal(msgBatch, batch); err != nil {
		return err
	}

	switch enum.TaskType(batch.TaskType) {
	case enum.T2:
		return a.refDatasetStore.AggregateDataTask2(a.SendAggregateDataBestSelling, a.SendAggregateDataMostProfits)
	case enum.T3:
		return a.refDatasetStore.AggregateDataTask3(a.SendAggregateDataTask3)
	case enum.T4:
		return a.refDatasetStore.AggregateDataTask4(a.SendAggregateDataTask4)
	default:
		return fmt.Errorf("unknown task type: %v", batch.TaskType)
	}
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

func (a *Aggregator) SendAggregateDataTask4(items cache.MapJoinMostPurchasesUser) error {
	// TODO: Sacar a config.yaml
	batchSize := 100
	var currentBatch []*joined.JoinMostPurchasesUser

	gatewayQueue, err := StartQueueMiddleware(a.config.GatewayAddress, a.config.GatewayControllerDataQueue)
	if err != nil {
		return err
	}

	for _, item := range items {
		currentBatch = append(currentBatch, item)
		if len(currentBatch) >= batchSize {
			specificBatch := &joined.JoinMostPurchasesUserBatch{
				Users: currentBatch,
			}

			sendErr := SendBatchToGateway(specificBatch, gatewayQueue, enum.T4)
			if sendErr != nil {
				return sendErr
			}

			currentBatch = []*joined.JoinMostPurchasesUser{}
		}
	}

	if len(currentBatch) > 0 {
		specificBatch := &joined.JoinMostPurchasesUserBatch{
			Users: currentBatch,
		}

		sendErr := SendBatchToGateway(specificBatch, gatewayQueue, enum.T4)
		if sendErr != nil {
			return sendErr
		}
	}

	err = SendDoneBatchToGateway(gatewayQueue, enum.T4)
	if err != nil {
		return err
	}

	return nil
}

func (a *Aggregator) SendAggregateDataTask3(items cache.MapJoinStoreTPV) error {
	// TODO: Sacar a config.yaml
	batchSize := 100
	var currentBatch []*joined.JoinStoreTPV

	gatewayQueue, err := StartQueueMiddleware(a.config.GatewayAddress, a.config.GatewayControllerDataQueue)
	if err != nil {
		return err
	}

	for _, item := range items {
		currentBatch = append(currentBatch, item)
		if len(currentBatch) >= batchSize {
			specificBatch := &joined.JoinStoreTPVBatch{
				Items: currentBatch,
			}

			sendErr := SendBatchToGateway(specificBatch, gatewayQueue, enum.T3)
			if sendErr != nil {
				return sendErr
			}

			currentBatch = []*joined.JoinStoreTPV{}
		}
	}

	if len(currentBatch) > 0 {
		specificBatch := &joined.JoinStoreTPVBatch{
			Items: currentBatch,
		}

		sendErr := SendBatchToGateway(specificBatch, gatewayQueue, enum.T3)
		if sendErr != nil {
			return sendErr
		}
	}

	err = SendDoneBatchToGateway(gatewayQueue, enum.T3)
	if err != nil {
		return err
	}

	return nil
}

func (a *Aggregator) SendAggregateDataBestSelling(items cache.MapJoinBestSelling) error {
	// TODO: Sacar a config.yaml
	batchSize := 100
	var currentBatch []*joined.JoinBestSellingProducts

	gatewayQueue, err := StartQueueMiddleware(a.config.GatewayAddress, a.config.GatewayControllerDataQueue)
	if err != nil {
		return err
	}

	for _, item := range items {
		currentBatch = append(currentBatch, item)
		if len(currentBatch) >= batchSize {
			specificBatch := &joined.JoinBestSellingProductsBatch{
				Items: currentBatch,
			}

			sendErr := SendBatchToGateway(specificBatch, gatewayQueue, enum.T2)
			if sendErr != nil {
				return sendErr
			}

			currentBatch = []*joined.JoinBestSellingProducts{}
		}
	}

	if len(currentBatch) > 0 {
		specificBatch := &joined.JoinBestSellingProductsBatch{
			Items: currentBatch,
		}

		sendErr := SendBatchToGateway(specificBatch, gatewayQueue, enum.T2)
		if sendErr != nil {
			return sendErr
		}
	}

	err = SendDoneBatchToGateway(gatewayQueue, enum.T2)
	if err != nil {
		return err
	}

	return nil
}

func (a *Aggregator) SendAggregateDataMostProfits(items cache.MapJoinMostProfits) error {
	// TODO: Sacar a config.yaml
	batchSize := 100
	var currentBatch []*joined.JoinMostProfitsProducts

	gatewayQueue, err := StartQueueMiddleware(a.config.GatewayAddress, a.config.GatewayControllerDataQueue)
	if err != nil {
		return err
	}

	for _, item := range items {
		currentBatch = append(currentBatch, item)
		if len(currentBatch) >= batchSize {
			specificBatch := &joined.JoinMostProfitsProductsBatch{
				Items: currentBatch,
			}

			sendErr := SendBatchToGateway(specificBatch, gatewayQueue, enum.T2)
			if sendErr != nil {
				return sendErr
			}

			currentBatch = []*joined.JoinMostProfitsProducts{}
		}
	}

	if len(currentBatch) > 0 {
		specificBatch := &joined.JoinMostProfitsProductsBatch{
			Items: currentBatch,
		}

		sendErr := SendBatchToGateway(specificBatch, gatewayQueue, enum.T2)
		if sendErr != nil {
			return sendErr
		}
	}

	err = SendDoneBatchToGateway(gatewayQueue, enum.T2)
	if err != nil {
		return err
	}

	return nil
}

func SendBatchToGateway(batch proto.Message, gatewayQueue middleware.MessageMiddleware, taskType enum.TaskType) error {
	payload, marshalErr := proto.Marshal(batch)
	if marshalErr != nil {
		return marshalErr
	}

	dataBatch := &data_batch.DataBatch{
		TaskType: int32(taskType),
		Done:     false,
		Payload:  payload,
	}

	sendErr := SendDataBatch(dataBatch, gatewayQueue)
	if sendErr != nil {
		return sendErr
	}

	return nil
}

func SendDoneBatchToGateway(gatewayQueue middleware.MessageMiddleware, taskType enum.TaskType) error {
	dataBatch := &data_batch.DataBatch{
		TaskType: int32(taskType),
		Done:     true,
	}

	sendErr := SendDataBatch(dataBatch, gatewayQueue)
	if sendErr != nil {
		return sendErr
	}

	return nil
}
