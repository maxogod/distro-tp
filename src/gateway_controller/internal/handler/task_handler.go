package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/controller_connection"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/gateway_controller/business"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/workers_manager"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type TaskHandler struct {
	ControllerService *business.GatewayControllerService
	taskHandlers      map[enum.TaskType]func(*data_batch.DataBatch) error

	middlewareUrl string

	filtersQueueMiddleware       middleware.MessageMiddleware
	joinerRefDataQueue           middleware.MessageMiddleware
	processedDataQueueMiddleware middleware.MessageMiddleware
	workersFinishQueues          map[enum.WorkerType]middleware.MessageMiddleware

	nodeConnections middleware.MessageMiddleware
	workerManager   workers_manager.WorkersManager

	getWorkerStatusChan chan bool
}

func NewTaskHandler(controllerService *business.GatewayControllerService, url string) Handler {
	th := &TaskHandler{
		ControllerService: controllerService,
	}

	th.middlewareUrl = url

	// TODO pass address here somehow or instanciate somewhere else
	th.filtersQueueMiddleware = middleware.GetFilterQueue(url)
	th.joinerRefDataQueue = middleware.GetJoinerQueue(url)
	th.processedDataQueueMiddleware = middleware.GetProcessedDataQueue(url)
	th.workersFinishQueues = make(map[enum.WorkerType]middleware.MessageMiddleware)
	th.workersFinishQueues[enum.Filter] = middleware.GetFilterQueue(url)
	th.workersFinishQueues[enum.GroupBy] = middleware.GetGroupByQueue(url)
	th.workersFinishQueues[enum.Reducer] = middleware.GetReducerQueue(url)
	th.workersFinishQueues[enum.Joiner] = middleware.GetJoinerQueue(url)
	th.workersFinishQueues[enum.Aggregator] = middleware.GetAggregatorQueue(url)

	th.nodeConnections = middleware.GetNodeConnectionsQueue(url)
	th.workerManager = workers_manager.NewWorkersManager(url)

	th.taskHandlers = map[enum.TaskType]func(*data_batch.DataBatch) error{
		enum.T1: th.handleTaskType1,
		enum.T2: th.handleTaskType2,
		enum.T3: th.handleTaskType3,
		enum.T4: th.handleTaskType4,
	}

	th.getWorkerStatusChan = make(chan bool, 1)
	go th.getWorkerStatus()

	return th
}

func (th *TaskHandler) HandleTask(taskType enum.TaskType, dataBatch *data_batch.DataBatch) error {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		return fmt.Errorf("unknown task type: %d", taskType)
	}

	return handler(dataBatch)
}

func (th *TaskHandler) HandleReferenceData(dataBatch *data_batch.DataBatch, currentClientID string) error {
	log.Debugf("Received reference data")
	dataBatch.ClientId = currentClientID

	serializedRefData, err := proto.Marshal(dataBatch)
	if err != nil {
		return err
	}
	th.joinerRefDataQueue.Send(serializedRefData)

	return nil
}

func (th *TaskHandler) SendDone(taskType enum.TaskType, currentClientID string) error {
	th.getWorkerStatusChan <- true
	log.Debug("Sending done signal to workers")
	nodeConnections := middleware.GetNodeConnectionsQueue(th.middlewareUrl)
	finishQueue := th.workersFinishQueues[enum.Filter]

	doneBatch := &data_batch.DataBatch{
		TaskType: int32(taskType),
		ClientId: currentClientID,
		Done:     true,
	}
	serializedDoneBatch, err := proto.Marshal(doneBatch)
	if err != nil {
		return err
	}
	finishQueue.Send(serializedDoneBatch)

	done := make(chan bool)
	nodeConnections.StartConsuming(func(ch middleware.ConsumeChannel, d chan error) {
		log.Debug("Started listening for worker finished messages")
		for msg := range ch {
			log.Debug("Received worker finished message")

			workerConn := &controller_connection.ControllerConnection{}
			err := proto.Unmarshal(msg.Body, workerConn)
			if err != nil {
				continue
			}

			if !workerConn.Finished {
				continue
			} else if workerConn.GetClientId() != currentClientID {
				msg.Ack(false)
				log.Debugf("Ignoring finished message from worker %s for different client %s (current client %s)", workerConn.WorkerName, workerConn.GetClientId(), currentClientID)
				continue
			}

			err = th.workerManager.FinishWorker(workerConn.WorkerName)
			if err != nil {
				continue
			}
			msg.Ack(false)

			// Refresh exchange topic in case all workers of a stage are finished
			finishTopic, allFinished := th.workerManager.GetNextWorkerStageToFinish()
			if allFinished {
				break
			}

			finishQueue, exists := th.workersFinishQueues[finishTopic]
			if !exists {
				log.Errorf("No finish queue for worker type %s", finishTopic)
				return
			}
			finishQueue.Send(serializedDoneBatch)
		}
		done <- true
	})
	<-done

	nodeConnections.StopConsuming()
	nodeConnections.Close()

	return nil
}

func (th *TaskHandler) GetReportData(data chan []byte, disconnect chan bool) {
	log.Debug("Starting to get report data")
	defer th.processedDataQueueMiddleware.StopConsuming()

	done := make(chan bool)
	th.processedDataQueueMiddleware.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		log.Debug("Started listening for processed data")
		for {
			select {
			case msg := <-msgs:
				data <- msg.Body
				msg.Ack(false)
			case <-done:
				return
			}
		}
	})

	// Wait until disconnection signal
	<-disconnect
	done <- true
}

func (th *TaskHandler) handleTaskType1(dataBatch *data_batch.DataBatch) error {
	// this task only requires created_at and final_amount to remain
	removeColumns := []string{"voucher_id", "discount_applied", "payment_method", "original_amount", "user_id", "store_id"}
	cleanedData, err := th.processTransaction(dataBatch.GetPayload(), removeColumns)
	if err != nil {
		return err
	}
	return th.sendCleanedDataToFilterQueue(dataBatch, cleanedData)
}

func (th *TaskHandler) handleTaskType2(dataBatch *data_batch.DataBatch) error {
	removeColumns := []string{"transaction_id", "unit_price"}
	cleanedData, err := th.processTransactionItems(dataBatch.GetPayload(), removeColumns)
	if err != nil {
		return err
	}
	return th.sendCleanedDataToFilterQueue(dataBatch, cleanedData)
}

func (th *TaskHandler) handleTaskType3(dataBatch *data_batch.DataBatch) error {
	removeColumns := []string{"voucher_id", "discount_applied", "payment_method", "original_amount", "user_id"}
	cleanedData, err := th.processTransaction(dataBatch.GetPayload(), removeColumns)
	if err != nil {
		return err
	}
	return th.sendCleanedDataToFilterQueue(dataBatch, cleanedData)
}

func (th *TaskHandler) handleTaskType4(dataBatch *data_batch.DataBatch) error {
	removeColumns := []string{"voucher_id", "discount_applied", "payment_method", "original_amount"}
	cleanedData, err := th.processTransaction(dataBatch.GetPayload(), removeColumns)
	if err != nil {
		return err
	}
	return th.sendCleanedDataToFilterQueue(dataBatch, cleanedData)
}

func (th *TaskHandler) processTransaction(
	payload []byte,
	removeColumns []string,
) (*raw.TransactionBatch, error) {

	transactions := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactions)

	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %v", err)
	}
	th.ControllerService.CleanTransactionData(transactions.Transactions, removeColumns)
	return transactions, err
}

func (th *TaskHandler) processTransactionItems(
	payload []byte,
	removeColumns []string,
) (*raw.TransactionItemsBatch, error) {
	items := &raw.TransactionItemsBatch{}
	err := proto.Unmarshal(payload, items)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %v", err)
	}
	cleanedData, err := th.ControllerService.CleanTransactionItemData(items.TransactionItems, removeColumns)
	return &raw.TransactionItemsBatch{TransactionItems: cleanedData}, err
}

func (th *TaskHandler) sendCleanedDataToFilterQueue(dataBatch *data_batch.DataBatch, cleanedData proto.Message) error {
	data, err := proto.Marshal(cleanedData)
	if err != nil {
		return err
	}

	dataBatch.Payload = data
	serializedPayload, err := proto.Marshal(dataBatch)
	if err != nil {
		return err
	}

	th.filtersQueueMiddleware.Send(serializedPayload)

	return nil
}

func (th *TaskHandler) getWorkerStatus() {
	defer th.nodeConnections.StopConsuming()

	done := make(chan bool)
	th.nodeConnections.StartConsuming(func(ch middleware.ConsumeChannel, d chan error) {
		for {
			var msg middleware.MessageDelivery
			select {
			case <-th.getWorkerStatusChan:
				done <- true
				return
			case msg = <-ch:
			}

			nodeConn := &controller_connection.ControllerConnection{}
			err := proto.Unmarshal(msg.Body, nodeConn)
			if err != nil {
				// TODO: should ack not be sent?
				continue
			}
			if !nodeConn.Finished {
				// Save and ack if is a worker announcement
				err := th.workerManager.AddWorker(nodeConn.WorkerName)
				if err != nil {
					continue
				}
				msg.Ack(false)
			}
		}
	})
	<-done
}

func (th *TaskHandler) Reset() {
	th.workerManager.ClearStatus()
	close(th.getWorkerStatusChan)
	th.getWorkerStatusChan = make(chan bool, 1)
	go th.getWorkerStatus()
}

func (th *TaskHandler) Close() {
	th.filtersQueueMiddleware.Close()
	th.joinerRefDataQueue.Close()
	th.processedDataQueueMiddleware.Close()
	th.nodeConnections.Close()
	for _, q := range th.workersFinishQueues {
		q.Close()
	}
	th.getWorkerStatusChan <- true
}
