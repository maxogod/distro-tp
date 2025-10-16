package worker

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
)

const FINISH enum.TaskType = 0

// Before creating a TaskHandler, a TaskExecutor is required to be implemented
// for the specific tasks that the worker will handle via the TaskHandler.
// Once created, it is passed to the TaskHandler constructor
type TaskExecutor interface {
	HandleTask1(payload []byte, clientID string) error
	HandleTask2(payload []byte, clientID string) error
	HandleTask2_1(payload []byte, clientID string) error
	HandleTask2_2(payload []byte, clientID string) error
	HandleTask3(payload []byte, clientID string) error
	HandleTask4(payload []byte, clientID string) error
	HandleFinishClient(clientID string) error
	Close() error
}

// A generic implementation of DataHandler that routes tasks to specific handlers based on the task type
// This is not required for every worker, but highly recommended to use it
// You only create this struct and pass it to the MessageHandler
type taskHandler struct {
	taskHandlers map[enum.TaskType]func([]byte, string) error
	taskExecutor TaskExecutor
}

func NewTaskHandler(
	taskExecutor TaskExecutor) DataHandler {
	th := &taskHandler{
		taskExecutor: taskExecutor,
	}

	th.taskHandlers = map[enum.TaskType]func([]byte, string) error{
		enum.T1:   th.taskExecutor.HandleTask1,
		enum.T2:   th.taskExecutor.HandleTask2,
		enum.T2_1: th.taskExecutor.HandleTask2_1,
		enum.T2_2: th.taskExecutor.HandleTask2_2,
		enum.T3:   th.taskExecutor.HandleTask3,
		enum.T4:   th.taskExecutor.HandleTask4,
	}

	return th
}

func (th *taskHandler) HandleData(dataEnvelope *protocol.DataEnvelope) error {
	taskType := enum.TaskType(dataEnvelope.GetTaskType())
	payload := dataEnvelope.GetPayload()
	clientID := dataEnvelope.GetClientId()
	return th.handleTask(taskType, payload, clientID)
}

func (th *taskHandler) handleTask(taskType enum.TaskType, payload []byte, clientID string) error {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		if taskType == FINISH {
			return nil
		}
		return fmt.Errorf("unknown task type: %d", taskType)
	}
	return handler(payload, clientID)
}

func (th *taskHandler) HandleFinishClient(clientID string) error {
	return th.taskExecutor.HandleFinishClient(clientID)
}

func (th *taskHandler) Close() error {
	return th.taskExecutor.Close()
}
