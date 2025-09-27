package handler

import (
	"coffee-analisis/src/common/models"
	"coffee-analisis/src/filter/business"
	"fmt"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("task-handler")

type TaskHandler struct {
	FilterService *business.FilterService
	// TODO: this should be replace with a protocol buffer that when given the task type, then process the payload insted of
	// directly giving the task type and payload
	taskHandlers map[models.TaskType]func(any) (any, error)
	TaskConfig   *TaskConfig
}

func NewTaskHandler(filterService *business.FilterService, taskConfig *TaskConfig) *TaskHandler {
	th := &TaskHandler{
		FilterService: filterService,
		TaskConfig:    taskConfig,
	}

	th.taskHandlers = map[models.TaskType]func(any) (any, error){
		models.T1: th.handleTaskType1,
		models.T2: th.handleTaskType2,
		models.T3: th.handleTaskType3,
		models.T4: th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType models.TaskType, payload any) (any, error) {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		return nil, fmt.Errorf("unknown task type: %d", taskType)
	}

	log.Infof("Processing task type: %d", taskType)
	return handler(payload)
}

func (th *TaskHandler) handleTaskType1(payload any) (any, error) {
	log.Debug("Handling Task Type 1")

	transactions, ok := payload.([]models.Transaction)
	if !ok {
		return nil, fmt.Errorf("task T1 expects a batch of Transactions, got %T", payload)
	}

	result := business.FilterByYearBetween(th.TaskConfig.FilterYearFrom, th.TaskConfig.FilterYearTo, transactions)
	result = business.FilterByHourBetween(th.TaskConfig.BusinessHourFrom, th.TaskConfig.BusinessHourTo, result)
	result = business.FilterByTotalAmountGreaterThan(th.TaskConfig.TotalAmountThreshold, result)
	return result, nil
}

func (th *TaskHandler) handleTaskType2(payload any) (any, error) {
	log.Debug("Handling Task Type 2")

	items, ok := payload.([]models.TransactionItem)
	if !ok {
		return nil, fmt.Errorf("task T2 expects a batch of Transaction Item, got %T", payload)
	}

	result := business.FilterByYearBetween(th.TaskConfig.FilterYearFrom, th.TaskConfig.FilterYearTo, items)
	return result, nil
}

func (th *TaskHandler) handleTaskType3(payload any) (any, error) {
	log.Debug("Handling Task Type 3")

	transactions, ok := payload.([]models.Transaction)
	if !ok {
		return nil, fmt.Errorf("task T3 expects a batch of Transactions, got %T", payload)
	}

	result := business.FilterByYearBetween(th.TaskConfig.FilterYearFrom, th.TaskConfig.FilterYearTo, transactions)
	result = business.FilterByHourBetween(th.TaskConfig.BusinessHourFrom, th.TaskConfig.BusinessHourTo, result)
	return result, nil
}

func (th *TaskHandler) handleTaskType4(payload any) (any, error) {
	log.Debug("Handling Task Type 4")

	transactions, ok := payload.([]models.Transaction)
	if !ok {
		return nil, fmt.Errorf("task T4 expects a batch of Transactions, got %T", payload)
	}

	result := business.FilterByYearBetween(th.TaskConfig.FilterYearFrom, th.TaskConfig.FilterYearTo, transactions)
	return result, nil
}
