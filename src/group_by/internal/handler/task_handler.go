package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/group_by/business"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type TaskHandler struct {
	groupByService *business.GroupByService
	queueHandler   *MessageHandler
	taskHandlers   map[enum.TaskType]func([]byte) error
}

func NewTaskHandler(
	groupByService *business.GroupByService,
	queueHandler *MessageHandler) *TaskHandler {
	th := &TaskHandler{
		groupByService: groupByService,
		queueHandler:   queueHandler,
	}

	th.taskHandlers = map[enum.TaskType]func([]byte) error{
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

func (th *TaskHandler) handleTaskType2(payload []byte) error {
	items, err := utils.GetTransactionItems(payload)
	if err != nil {
		return err
	}
	groupedItems := th.groupByService.GroupItemsByYearMonthAndItem(items)
	return th.sendGroupedToQueue(groupedItems, enum.T2)
}

func (th *TaskHandler) handleTaskType3(payload []byte) error {
	transactions, err := utils.GetTransactions(payload)
	if err != nil {
		return err
	}
	groupedTransactions := th.groupByService.GroupItemsBySemesterAndStore(transactions)
	return th.sendGroupedToQueue(groupedTransactions, enum.T3)
}

func (th *TaskHandler) handleTaskType4(payload []byte) error {
	transactions, err := utils.GetTransactions(payload)
	if err != nil {
		return err
	}
	groupedTransactions := th.groupByService.GroupTransactionByUserAndStore(transactions)
	return th.sendGroupedToQueue(groupedTransactions, enum.T4)
}

func (th *TaskHandler) sendGroupedToQueue(
	grouped interface{},
	taskType enum.TaskType,
) error {
	switch groups := grouped.(type) {
	case map[string][]*raw.TransactionItems:
		for _, itemsGroup := range groups {
			groupBatch := &raw.TransactionItemsBatch{
				TransactionItems: itemsGroup,
			}
			serialized, err := proto.Marshal(groupBatch)
			if err != nil {
				return err
			}
			if err := th.queueHandler.SendData(taskType, serialized); err != nil {
				return err
			}
		}
	case map[string][]*raw.Transaction:
		for _, transactionGroup := range groups {
			groupBatch := &raw.TransactionBatch{
				Transactions: transactionGroup,
			}
			serialized, err := proto.Marshal(groupBatch)
			if err != nil {
				return err
			}
			if err := th.queueHandler.SendData(taskType, serialized); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported group type")
	}
	return nil
}
