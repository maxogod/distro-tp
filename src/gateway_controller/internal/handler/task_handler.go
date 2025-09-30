package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/gateway_controller/business"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type Handler interface {
	handleTaskType1(payload []byte) error
	handleTaskType2(payload []byte) error
	handleTaskType3(payload []byte) error
	handleTaskType4(payload []byte) error
	HandleTask(taskType enum.TaskType, payload []byte) error
	HandleReferenceData(payload []byte) error
	SendDone() error
	GetReportData() []byte
}

type TaskHandler struct {
	ControllerService *business.GatewayControllerService
	taskHandlers      map[enum.TaskType]func([]byte) error
}

func NewTaskHandler(controllerService *business.GatewayControllerService) *TaskHandler {
	th := &TaskHandler{
		ControllerService: controllerService,
	}

	th.taskHandlers = map[enum.TaskType]func([]byte) error{
		enum.T1: th.handleTaskType1,
		enum.T2: th.handleTaskType2,
		enum.T3: th.handleTaskType3,
		enum.T4: th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType enum.TaskType, payload []byte) error {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		return fmt.Errorf("unknown task type: %d", taskType)
	}

	return handler(payload)
}

func (th *TaskHandler) HandleReferenceData(payload []byte) error {

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

func (th *TaskHandler) handleTaskType1(payload []byte) error {
	// this task only requires created_at and final_amount to remain
	removeColumns := []string{"voucher_id", "discount_applied", "payment_method", "original_amount", "user_id", "store_id"}
	cleanedData, err := th.processTransaction(payload, removeColumns)
	if err != nil {
		return err
	}
	log.Debugf("Cleaned data: %+v", cleanedData)
	return nil
}

func (th *TaskHandler) handleTaskType2(payload []byte) error {
	removeColumns := []string{"transaction_id", "unit_price"}
	cleanedData, err := th.processTransactionItems(payload, removeColumns)
	if err != nil {
		return err
	}
	log.Debugf("Cleaned data: %+v", cleanedData)
	return nil
}

func (th *TaskHandler) handleTaskType3(payload []byte) error {
	removeColumns := []string{"voucher_id", "discount_applied", "payment_method", "original_amount", "user_id"}
	cleanedData, err := th.processTransaction(payload, removeColumns)
	if err != nil {
		return err
	}
	log.Debugf("Cleaned data: %+v", cleanedData)
	return nil
}

func (th *TaskHandler) handleTaskType4(payload []byte) error {
	removeColumns := []string{"voucher_id", "discount_applied", "payment_method", "original_amount"}
	cleanedData, err := th.processTransaction(payload, removeColumns)
	if err != nil {
		return err
	}
	log.Debugf("Cleaned data: %+v", cleanedData)
	return nil
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
