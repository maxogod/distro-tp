package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/filter/business"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type TaskHandler struct {
	FilterService *business.FilterService
	queueHandler  *MessageHandler
	TaskConfig    *TaskConfig
	taskHandlers  map[enum.TaskType]func([]byte) error
}

func NewTaskHandler(
	filterService *business.FilterService,
	queueHandler *MessageHandler,
	taskConfig *TaskConfig) *TaskHandler {
	th := &TaskHandler{
		FilterService: filterService,
		queueHandler:  queueHandler,
		TaskConfig:    taskConfig,
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

func (th *TaskHandler) handleTaskType1(payload []byte) error {
	transactions, err := utils.GetTransactions(payload)
	if err != nil {
		return err
	}

	result := business.FilterByYearBetween(th.TaskConfig.FilterYearFrom, th.TaskConfig.FilterYearTo, transactions)
	result = business.FilterByHourBetween(th.TaskConfig.BusinessHourFrom, th.TaskConfig.BusinessHourTo, result)
	result = business.FilterByTotalAmountGreaterThan(th.TaskConfig.TotalAmountThreshold, result)

	filteredBatch := &raw.TransactionBatch{
		Transactions: result,
	}

	serializedTransactions, err := proto.Marshal(filteredBatch)
	if err != nil {
		return err
	}
	// Sends the transactions to the process data queue
	err = th.queueHandler.SendData(enum.T1, serializedTransactions)
	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType2(payload []byte) error {

	items, err := utils.GetTransactionItems(payload)
	if err != nil {
		return err
	}

	result := business.FilterByYearBetween(th.TaskConfig.FilterYearFrom, th.TaskConfig.FilterYearTo, items)

	filteredBatch := &raw.TransactionItemsBatch{
		TransactionItems: result,
	}

	serializedItems, err := proto.Marshal(filteredBatch)
	if err != nil {
		return err
	}
	// Sends the transaction items to both reducer queues
	err = th.queueHandler.SendData(enum.T2, serializedItems)
	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType3(payload []byte) error {

	transactions, err := utils.GetTransactions(payload)
	if err != nil {
		return err
	}

	result := business.FilterByYearBetween(th.TaskConfig.FilterYearFrom, th.TaskConfig.FilterYearTo, transactions)
	result = business.FilterByHourBetween(th.TaskConfig.BusinessHourFrom, th.TaskConfig.BusinessHourTo, result)

	filteredBatch := &raw.TransactionBatch{
		Transactions: result,
	}

	serializedTransactions, err := proto.Marshal(filteredBatch)
	if err != nil {
		return err
	}
	// Sends the transactions to the reduce sum queue
	err = th.queueHandler.SendData(enum.T3, serializedTransactions)
	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType4(payload []byte) error {

	transactions, err := utils.GetTransactions(payload)
	if err != nil {
		return err
	}

	result := business.FilterByYearBetween(th.TaskConfig.FilterYearFrom, th.TaskConfig.FilterYearTo, transactions)

	filteredBatch := &raw.TransactionBatch{
		Transactions: result,
	}

	serializedTransactions, err := proto.Marshal(filteredBatch)
	if err != nil {
		return err
	}
	// Sends the transactions to the reduce count queue
	err = th.queueHandler.SendData(enum.T4, serializedTransactions)
	if err != nil {
		return err
	}

	return nil
}
