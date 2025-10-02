package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/reducer/business"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type TaskHandler struct {
	reducerService *business.ReducerService
	queueHandler   *MessageHandler
	taskHandlers   map[enum.TaskType]func([]byte) error
}

func NewTaskHandler(
	reducerService *business.ReducerService,
	queueHandler *MessageHandler) *TaskHandler {
	th := &TaskHandler{
		reducerService: reducerService,
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
	bestSelling := th.reducerService.SumMostProfitsProducts(items)
	serialized, err := proto.Marshal(bestSelling)
	errBestSelling := th.queueHandler.SendData(enum.T2_2, serialized)

	mostProfit := th.reducerService.SumBestSellingProducts(items)
	serialized, err = proto.Marshal(mostProfit)
	errMostProfit := th.queueHandler.SendData(enum.T2_1, serialized)

	if errBestSelling != nil || errMostProfit != nil {
		return fmt.Errorf("error sending data to queue")
	}

	return nil
}

func (th *TaskHandler) handleTaskType3(payload []byte) error {
	transactions, err := utils.GetTransactions(payload)
	if err != nil {
		return err
	}
	reducedTransactions := th.reducerService.SumTPV(transactions)
	serialized, err := proto.Marshal(reducedTransactions)
	return th.queueHandler.SendData(enum.T3, serialized)
}

func (th *TaskHandler) handleTaskType4(payload []byte) error {
	transactions, err := utils.GetTransactions(payload)
	if err != nil {
		return err
	}
	reducedTransactions := th.reducerService.CountMostPurchasesByUser(transactions)
	serialized, err := proto.Marshal(reducedTransactions)
	return th.queueHandler.SendData(enum.T4, serialized)
}
