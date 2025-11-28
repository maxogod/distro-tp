package worker

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
)

const FINISH enum.TaskType = 0

// Before creating a TaskHandler, a TaskExecutor is required to be implemented
// for the specific tasks that the worker will handle via the TaskHandler.
// Once created, it is passed to the TaskHandler constructor
type TaskExecutor interface {
	HandleTask1(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error
	HandleTask2(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error
	HandleTask3(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error
	HandleTask4(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error
	HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error
	Close() error
}

// A generic implementation of DataHandler that routes tasks to specific handlers based on the task type
// This is not required for every worker, but highly recommended to use it
// You only create this struct and pass it to the MessageHandler
type taskHandler struct {
	taskHandlers         map[enum.TaskType]func(*protocol.DataEnvelope, func(bool, bool) error) error
	sequencesPerClient   map[string]map[int32]bool
	taskExecutor         TaskExecutor
	shouldDropDuplicates bool

	messagesReceived       map[string]int32
	totalMessagesToReceive map[string]int32
}

func NewTaskHandler(taskExecutor TaskExecutor, shouldDropDuplicates bool) DataHandler {
	th := &taskHandler{
		taskExecutor:           taskExecutor,
		sequencesPerClient:     make(map[string]map[int32]bool),
		messagesReceived:       make(map[string]int32),
		totalMessagesToReceive: make(map[string]int32),
		shouldDropDuplicates:   shouldDropDuplicates,
	}

	th.taskHandlers = map[enum.TaskType]func(*protocol.DataEnvelope, func(bool, bool) error) error{
		enum.T1: th.taskExecutor.HandleTask1,
		enum.T2: th.taskExecutor.HandleTask2,
		enum.T3: th.taskExecutor.HandleTask3,
		enum.T4: th.taskExecutor.HandleTask4,
	}

	return th
}

func (th *taskHandler) HandleData(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	clientID := dataEnvelope.GetClientId()
	seqNum := dataEnvelope.GetSequenceNumber()

	if th.shouldDropDuplicates {
		seqs, ok := th.sequencesPerClient[clientID]
		if !ok || seqs == nil {
			seqs = make(map[int32]bool)
			th.sequencesPerClient[clientID] = seqs
		}
		if seqs[seqNum] {
			logger.Logger.Debugf("[%s] Duplicate sequence number %d. Ignoring message.", clientID, seqNum)
			return ackHandler(false, false)
		}

		seqs[seqNum] = true
	}

	taskType := enum.TaskType(dataEnvelope.GetTaskType())
	err := th.handleTask(taskType, dataEnvelope, ackHandler)
	if err != nil {
		return err
	}

	count, exists := th.messagesReceived[clientID]
	if exists {
		th.messagesReceived[clientID] = count + 1
	} else {
		th.messagesReceived[clientID] = 1
	}

	if total, exists := th.totalMessagesToReceive[clientID]; exists && total != 0 && th.messagesReceived[clientID] == total {
		return th.HandleFinishClient(dataEnvelope, func(bool, bool) error { return nil }) // TODO: what if it dies here
	}

	return nil
}

func (th *taskHandler) handleTask(taskType enum.TaskType, dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		if taskType == FINISH {
			return nil
		}
		return fmt.Errorf("unknown task type: %d", taskType)
	}
	return handler(dataEnvelope, ackHandler)
}

func (th *taskHandler) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	// Remove client sequences tracking
	clientID := dataEnvelope.GetClientId()

	count, existsCount := th.messagesReceived[clientID]
	if existsCount && count != dataEnvelope.GetTotalMessages() {
		if dataEnvelope.GetTotalMessages() != 0 {
			th.totalMessagesToReceive[clientID] = dataEnvelope.GetTotalMessages()
			ackHandler(true, false)
			return nil
		}
	}
	// If this is reached, then the total messages was met

	delete(th.sequencesPerClient, clientID)
	delete(th.messagesReceived, clientID)
	delete(th.totalMessagesToReceive, clientID)

	// Call executor finish client handler
	return th.taskExecutor.HandleFinishClient(dataEnvelope, ackHandler)
}

func (th *taskHandler) Close() error {
	return th.taskExecutor.Close()
}
