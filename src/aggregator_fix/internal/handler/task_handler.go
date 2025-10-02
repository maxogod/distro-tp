package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/aggregator_fix/business"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/utils"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type TaskHandler struct {
	aggregatorService *business.AggregatorService
	queueHandler      *MessageHandler
	taskHandlers      map[enum.TaskType]func([]byte) error
}

func NewTaskHandler(
	aggregatorService *business.AggregatorService,
	queueHandler *MessageHandler,
) *TaskHandler {
	th := &TaskHandler{
		aggregatorService: aggregatorService,
		queueHandler:      queueHandler,
	}

	th.taskHandlers = map[enum.TaskType]func([]byte) error{
		enum.T1:   th.handleTaskType1,
		enum.T2_1: th.handleTaskType2_1,
		enum.T2_2: th.handleTaskType2_2,
		enum.T3:   th.handleTaskType3,
		enum.T4:   th.handleTaskType4,
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

	// CHANGE WITH AGGREGATE LOGIC HERE
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
	// ====================================

	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType2_1(payload []byte) error {

	// CHANGE WITH AGGREGATE LOGIC HERE
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
	// ====================================

	return nil
}

func (th *TaskHandler) handleTaskType2_2(payload []byte) error {

	// CHANGE WITH AGGREGATE LOGIC HERE
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
	// ====================================

	return nil
}

func (th *TaskHandler) handleTaskType3(payload []byte) error {

	// CHANGE WITH AGGREGATE LOGIC HERE
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
	// ====================================

	return nil
}

func (th *TaskHandler) handleTaskType4(payload []byte) error {

	// CHANGE WITH AGGREGATE LOGIC HERE
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
	// ====================================

	return nil
}
