package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/joiner/business"
	"google.golang.org/protobuf/proto"
)

type TaskHandler struct {
	joinerService *business.JoinerService
	queueHandler  *MessageHandler
	taskHandlers  map[enum.TaskType]func([]byte) error
}

func NewTaskHandler(
	joinerService *business.JoinerService, queueHandler *MessageHandler) *TaskHandler {
	th := &TaskHandler{
		joinerService: joinerService,
		queueHandler:  queueHandler,
	}

	th.taskHandlers = map[enum.TaskType]func([]byte) error{
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

func (th *TaskHandler) handleTaskType2_1(payload []byte) error {

	reducedData, err := utils.UnmarshalPayload(payload, &reduced.BestSellingProducts{})
	if err != nil {
		return err
	}

	joinedResult, err := th.joinerService.JoinBestSellingProducts(reducedData)
	if err != nil {
		return err
	}

	serializedResults, err := proto.Marshal(joinedResult)
	if err != nil {
		return err
	}

	err = th.queueHandler.SendData(enum.T2_1, serializedResults)
	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType2_2(payload []byte) error {

	reducedData, err := utils.UnmarshalPayload(payload, &reduced.MostProfitsProducts{})
	if err != nil {
		return err
	}

	joinedResult, err := th.joinerService.JoinMostProfitsProducts(reducedData)
	if err != nil {
		return err
	}

	serializedResults, err := proto.Marshal(joinedResult)
	if err != nil {
		return err
	}

	err = th.queueHandler.SendData(enum.T2_2, serializedResults)
	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType3(payload []byte) error {

	reducedData, err := utils.UnmarshalPayload(payload, &reduced.StoreTPV{})
	if err != nil {
		return err
	}

	joinedResult, err := th.joinerService.JoinTPV(reducedData)
	if err != nil {
		return err
	}

	serializedResults, err := proto.Marshal(joinedResult)
	if err != nil {
		return err
	}

	err = th.queueHandler.SendData(enum.T3, serializedResults)
	if err != nil {
		return err
	}

	return nil
}

func (th *TaskHandler) handleTaskType4(payload []byte) error {

	reducedData, err := utils.UnmarshalPayload(payload, &reduced.MostPurchasesUser{})
	if err != nil {
		return err
	}

	joinedResult, err := th.joinerService.JoinMostPurchasesByUser(reducedData)
	if err != nil {
		return err
	}

	serializedResults, err := proto.Marshal(joinedResult)
	if err != nil {
		return err
	}

	err = th.queueHandler.SendData(enum.T4, serializedResults)
	if err != nil {
		return err
	}

	return nil
}
