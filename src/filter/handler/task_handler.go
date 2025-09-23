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
	taskHandlers map[models.TaskType]func(interface{}) (interface{}, error)
}

func NewTaskHandler() *TaskHandler {
	th := &TaskHandler{
		FilterService: business.NewFilterService(),
	}

	th.taskHandlers = map[models.TaskType]func(interface{}) (interface{}, error){
		models.T1: th.handleTaskType1,
		models.T2: th.handleTaskType2,
		models.T3: th.handleTaskType3,
		models.T4: th.handleTaskType4,
	}

	return th
}

func (th *TaskHandler) HandleTask(taskType models.TaskType, payload interface{}) (interface{}, error) {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		return nil, fmt.Errorf("unknown task type: %d", taskType)
	}

	log.Infof("Processing task type: %d", taskType)
	return handler(payload)
}

func (th *TaskHandler) handleTaskType1(payload interface{}) (interface{}, error) {
	log.Info("Handling Task Type 1 - Filter transactions by year 2023")

	transactions, ok := payload.([]models.Transaction)
	if !ok {
		return nil, fmt.Errorf("task T1 expects a batch of Transactions, got %T", payload)
	}

	result := business.FilterByYearBetween(2023, 2024, transactions)
	return result, nil
}

func (th *TaskHandler) handleTaskType2(payload interface{}) (interface{}, error) {
	log.Info("Handling Task Type 2 - Filter transaction items by amount >= 50")

	items, ok := payload.([]models.TransactionItem)
	if !ok {
		return nil, fmt.Errorf("task T2 expects a batch of Transaction Item, got %T", payload)
	}

	result := business.FilterByYearBetween(2023, 2024, items)
	return result, nil
}

func (th *TaskHandler) handleTaskType3(payload interface{}) (interface{}, error) {
	log.Info("Handling Task Type 3 - Filter transactions by business hours (9-17)")

	transactions, ok := payload.([]models.Transaction)
	if !ok {
		return nil, fmt.Errorf("task T3 expects a batch of Transactions, got %T", payload)
	}

	result := business.FilterByHourBetween(9, 17, transactions)
	return result, nil
}

func (th *TaskHandler) handleTaskType4(payload interface{}) (interface{}, error) {
	log.Info("Handling Task Type 4 - Filter transactions by amount >= 100")

	transactions, ok := payload.([]models.Transaction)
	if !ok {
		return nil, fmt.Errorf("task T4 expects a batch of Transactions, got %T", payload)
	}

	result := business.FilterByTotalAmountGreaterThan(100.0, transactions)
	return result, nil
}
