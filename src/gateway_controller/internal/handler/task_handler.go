package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/gateway_controller/business"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type Handler interface {
	handleTaskType1(dataBatch *data_batch.DataBatch) error
	handleTaskType2(dataBatch *data_batch.DataBatch) error
	handleTaskType3(dataBatch *data_batch.DataBatch) error
	handleTaskType4(dataBatch *data_batch.DataBatch) error
	HandleTask(taskType enum.TaskType, dataBatch *data_batch.DataBatch) error
	HandleReferenceData(dataBatch *data_batch.DataBatch) error
	SendDone() error
	GetReportData() []byte
}

const (
	JoinerQueue = "joiner"
	FilterQueue = "filter"
)

type TaskHandler struct {
	ControllerService     *business.GatewayControllerService
	taskHandlers          map[enum.TaskType]func(*data_batch.DataBatch) error
	filterQueueMiddleware middleware.MessageMiddleware
	joinerQueueMiddleware middleware.MessageMiddleware
}

func NewTaskHandler(controllerService *business.GatewayControllerService, url string) *TaskHandler {
	th := &TaskHandler{
		ControllerService: controllerService,
	}

	// TODO pass address here somehow or instanciate somewhere else
	filterQueueMiddleware, err := middleware.NewQueueMiddleware(url, FilterQueue)
	if err != nil {
		log.Errorf("Failed to create filter queue middleware: %v", err)
		return nil
	}
	joinerQueueMiddleware, err := middleware.NewQueueMiddleware(url, JoinerQueue)
	if err != nil {
		log.Errorf("Failed to create joiner queue middleware: %v", err)
		return nil
	}

	th.filterQueueMiddleware = filterQueueMiddleware
	th.joinerQueueMiddleware = joinerQueueMiddleware

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

	// TODO: send this to the joiner node
	log.Debugf("Received reference data")

	return nil

}

func (th *TaskHandler) SendDone() error {
	log.Info("All tasks processed. Sending done signal.")

	// TODO: broadcast done to each worker node

	return nil
}

func (th *TaskHandler) GetReportData() ([]byte, error) {

	// TODO: gather real report data from aggregator after sendDone is implemented

	report := "Report: All tasks completed successfully."
	log.Info(report)
	return []byte(report), nil
}

func (th *TaskHandler) handleTaskType1(dataBatch *data_batch.DataBatch) error {
	// this task only requires created_at and final_amount to remain
	removeColumns := []string{"voucher_id", "discount_applied", "payment_method", "original_amount", "user_id", "store_id"}
	cleanedData, err := th.processTransaction(dataBatch.GetPayload(), removeColumns)
	if err != nil {
		return err
	}
	log.Debugf("Cleaned data: %+v", cleanedData)

	return th.sendCleanedDataToFilterQueue(dataBatch, cleanedData)
}

func (th *TaskHandler) handleTaskType2(dataBatch *data_batch.DataBatch) error {
	removeColumns := []string{"transaction_id", "unit_price"}
	cleanedData, err := th.processTransactionItems(dataBatch.GetPayload(), removeColumns)
	if err != nil {
		return err
	}
	log.Debugf("Cleaned data: %+v", cleanedData)

	return th.sendCleanedDataToFilterQueue(dataBatch, cleanedData)
}

func (th *TaskHandler) handleTaskType3(dataBatch *data_batch.DataBatch) error {
	removeColumns := []string{"voucher_id", "discount_applied", "payment_method", "original_amount", "user_id"}
	cleanedData, err := th.processTransaction(dataBatch.GetPayload(), removeColumns)
	if err != nil {
		return err
	}
	log.Debugf("Cleaned data: %+v", cleanedData)
	return th.sendCleanedDataToFilterQueue(dataBatch, cleanedData)
}

func (th *TaskHandler) handleTaskType4(dataBatch *data_batch.DataBatch) error {
	removeColumns := []string{"voucher_id", "discount_applied", "payment_method", "original_amount"}
	cleanedData, err := th.processTransaction(dataBatch.GetPayload(), removeColumns)
	if err != nil {
		return err
	}
	log.Debugf("Cleaned data: %+v", cleanedData)
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
	log.Debugf("Cleaned data: %+v", cleanedData)
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

func (th *TaskHandler) Close() {
	th.filterQueueMiddleware.Close()
	th.joinerQueueMiddleware.Close()
}
