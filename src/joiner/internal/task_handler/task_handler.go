package task_handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/worker"
	"google.golang.org/protobuf/proto"
)

const FINISH enum.TaskType = 0

// taskHandler is responsible for handling incoming tasks and delegating them to the appropriate task executor methods.
// Used only in Joiner worker because of special needed handling.
type taskHandler struct {
	taskHandlers map[enum.TaskType]func([]byte, string) error
	taskExecutor worker.TaskExecutor
}

func NewTaskHandler(
	taskExecutor worker.TaskExecutor) worker.DataHandler {
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
	envelopePayload, err := proto.Marshal(dataEnvelope)
	if err != nil {
		return fmt.Errorf("failed to marshal data envelope: %w", err)
	}
	clientID := dataEnvelope.GetClientId()
	return th.handleTask(taskType, envelopePayload, clientID)
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
