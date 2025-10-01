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
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type TaskHandler struct {
	ControllerService              *business.GatewayControllerService
	taskHandlers                   map[enum.TaskType]func(*data_batch.DataBatch) error
	filterQueueMiddleware          middleware.MessageMiddleware
	joinerMenuItemsQueueMiddleware middleware.MessageMiddleware
	joinerStoreQueueMiddleware     middleware.MessageMiddleware
	joinerUsersQueueMiddleware     middleware.MessageMiddleware
	processedDataQueueMiddleware   middleware.MessageMiddleware

	nodeConnections    middleware.MessageMiddleware
	nodeConnectionsMap map[string]bool
}

func NewTaskHandler(controllerService *business.GatewayControllerService, url string) Handler {
	th := &TaskHandler{
		ControllerService: controllerService,
	}

	// TODO pass address here somehow or instanciate somewhere else
	th.filterQueueMiddleware = middleware.GetFilterQueue(url)
	th.joinerMenuItemsQueueMiddleware = middleware.GetMenuItemsQueue(url)
	th.joinerStoreQueueMiddleware = middleware.GetStoresQueue(url)
	th.joinerUsersQueueMiddleware = middleware.GetUsersQueue(url)
	th.processedDataQueueMiddleware = middleware.GetProcessedDataQueue(url)

	th.nodeConnections = middleware.GetNodeConnectionsQueue(url)

	th.taskHandlers = map[enum.TaskType]func(*data_batch.DataBatch) error{
		enum.T1: th.handleTaskType1,
		enum.T2: th.handleTaskType2,
		enum.T3: th.handleTaskType3,
		enum.T4: th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType enum.TaskType, dataBatch *data_batch.DataBatch) error {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		return fmt.Errorf("unknown task type: %d", taskType)
	}

	return handler(dataBatch)
}

func (th *TaskHandler) HandleReferenceData(dataBatch *data_batch.DataBatch) error {
	log.Debugf("Received reference data")

	serializedRefData, err := proto.Marshal(dataBatch)
	if err != nil {
		return err
	}

	switch enum.RefDatasetType(int(dataBatch.RefDataType)) {
	case enum.MenuItems:
		th.joinerMenuItemsQueueMiddleware.Send(serializedRefData)
	case enum.Stores:
		th.joinerStoreQueueMiddleware.Send(serializedRefData)
	case enum.Users:
		th.joinerUsersQueueMiddleware.Send(serializedRefData)
	}

	return nil
}

func (th *TaskHandler) SendDone() error {

	return nil
}

func (th *TaskHandler) GetReportData(data chan []byte, disconnect chan bool) {
	defer th.processedDataQueueMiddleware.StopConsuming()

	done := make(chan bool)
	th.processedDataQueueMiddleware.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		for msg := range msgs {
			data <- msg.Body
			msg.Ack(false)
			select {
			case <-done:
				return
			default:
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

	th.filterQueueMiddleware.Send(serializedPayload)
	return nil
}

func (th *TaskHandler) getWorkerStatus() {
	defer th.nodeConnections.StopConsuming()

	done := make(chan bool)
	th.nodeConnections.StartConsuming(func(ch middleware.ConsumeChannel, d chan error) {
		for msg := range ch {
			nodeConn := &controller_connection.ControllerConnection{}
			err := proto.Unmarshal(msg.Body, nodeConn)
			if err != nil {
				// TODO: should ack not be sent?
				continue
			}
			if !nodeConn.Finished {
				// Save and ack if is a worker announcement
				th.nodeConnectionsMap[nodeConn.WorkerName] = false
				msg.Ack(false)
			}
		}
		done <- true
	})
	<-done
}

func (th *TaskHandler) Close() {
	th.filterQueueMiddleware.Close()
	th.joinerUsersQueueMiddleware.Close()
	th.joinerStoreQueueMiddleware.Close()
	th.joinerMenuItemsQueueMiddleware.Close()
	th.processedDataQueueMiddleware.Close()
	th.nodeConnections.Close()
}
