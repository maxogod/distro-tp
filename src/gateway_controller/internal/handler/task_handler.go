package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/gateway_controller/business"
)

var log = logger.GetLogger()

type TaskHandler struct {
	ControllerService *business.GatewayControllerService
	taskHandlers      map[enum.TaskType]func(any) (any, error)
}

func NewTaskHandler(controllerService *business.GatewayControllerService) *TaskHandler {
	th := &TaskHandler{
		ControllerService: controllerService,
	}

	th.taskHandlers = map[enum.TaskType]func(any) (any, error){
		enum.T1: th.handleTaskType1,
		enum.T2: th.handleTaskType2,
		enum.T3: th.handleTaskType3,
		enum.T4: th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType enum.TaskType, payload any) (any, error) {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		return nil, fmt.Errorf("unknown task type: %d", taskType)
	}

	log.Debugf("Processing task type: %d", taskType)
	return handler(payload)
}

func (th *TaskHandler) handleTaskType1(payload any) (any, error) {

	// this task only requires created_at and final_amount to remain
	removeColumns := []string{
		"voucher_id",
		"discount_applied",
		"payment_method",
		"original_amount",
		"user_id",
		"store_id",
	}

	cleanedData, err := th.ControllerService.CleanTransactionData(payload.([]raw.Transaction), removeColumns)

	if err != nil {
		return nil, err
	}

	return cleanedData, nil
}

func (th *TaskHandler) handleTaskType2(payload any) (any, error) {

	// this task only requires created_at, subtotal, item_id and quantity to remain
	removeColumns := []string{
		"transaction_id",
		"unit_price",
	}

	cleanedData, err := th.ControllerService.CleanTransactionItemData(payload.([]raw.TransactionItems), removeColumns)

	if err != nil {
		return nil, err
	}

	return cleanedData, nil
}

func (th *TaskHandler) handleTaskType3(payload any) (any, error) {

	// this task only requires created_at, final_amount and the store_id to remain
	removeColumns := []string{
		"voucher_id",
		"discount_applied",
		"payment_method",
		"original_amount",
		"user_id",
	}

	cleanedData, err := th.ControllerService.CleanTransactionData(payload.([]raw.Transaction), removeColumns)

	if err != nil {
		return nil, err
	}

	return cleanedData, nil
}

func (th *TaskHandler) handleTaskType4(payload any) (any, error) {

	// this task only requires created_at, final_amount and the user_id, store_id foreign keys to remain
	removeColumns := []string{
		"voucher_id",
		"discount_applied",
		"payment_method",
		"original_amount",
	}

	cleanedData, err := th.ControllerService.CleanTransactionData(payload.([]raw.Transaction), removeColumns)

	if err != nil {
		return nil, err
	}

	return cleanedData, nil
}
