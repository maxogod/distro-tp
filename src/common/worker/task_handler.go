package worker

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
)

const FINISH enum.TaskType = 0
const REAP_AFTER_MSGS = 1000

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
	finishedClients      map[string]bool
	taskExecutor         TaskExecutor
	shouldDropDuplicates bool

	messagesReceived       map[string]int32
	totalMessagesToReceive map[string]int32
}

func NewTaskHandler(taskExecutor TaskExecutor, shouldDropDuplicates bool) DataHandler {
	th := &taskHandler{
		taskExecutor:           taskExecutor,
		sequencesPerClient:     make(map[string]map[int32]bool),
		finishedClients:        make(map[string]bool),
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
	if th.finishedClients[clientID] {
		logger.Logger.Debugf("[%s] Received message for finished client. Ignoring.", clientID)
		return ackHandler(false, false)
	}
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

	// logger.Logger.Debugf("[%s] COUNT - Messages received: %d", clientID, th.messagesReceived[clientID])
	if total, exists := th.totalMessagesToReceive[clientID]; exists && total != 0 && th.messagesReceived[clientID] == total {
		logger.Logger.Debugf("[%s] All messages received for client. Cleaning up.", clientID)
		th.finishedClients[clientID] = true
		th.reapFinishedClients(false)
		return th.HandleFinishClient(dataEnvelope, func(bool, bool) error { return nil }) // TODO: what if it dies here
	} else if th.messagesReceived[clientID]%REAP_AFTER_MSGS == 0 {
		th.reapFinishedClients(true)
	}

	return nil
}

func (th *taskHandler) handleTask(taskType enum.TaskType, dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	handler, exists := th.taskHandlers[taskType]
	if !exists {
		if taskType == FINISH { // TODO: is this necesary now?
			return nil
		}
		return fmt.Errorf("unknown task type: %d", taskType)
	}
	return handler(dataEnvelope, ackHandler)
}

func (th *taskHandler) reapFinishedClients(clearFinished bool) {
	for clientID := range th.finishedClients {
		delete(th.sequencesPerClient, clientID)
		delete(th.messagesReceived, clientID)
		delete(th.totalMessagesToReceive, clientID)
	}
	if clearFinished {
		clear(th.finishedClients)
	}
}

func (th *taskHandler) HandleFinishClient(dataEnvelope *protocol.DataEnvelope, ackHandler func(bool, bool) error) error {
	// Remove client sequences tracking
	clientID := dataEnvelope.GetClientId()
	count := th.messagesReceived[clientID]

	logger.Logger.Infof("[%s] Finished processing client in TaskHandler. Received %d/%d messages and %d sequences.",
		clientID, count, dataEnvelope.GetTotalMessages(), len(th.sequencesPerClient[clientID]))
	if dataEnvelope.GetTotalMessages() == 0 || count == dataEnvelope.GetTotalMessages() {
		th.finishedClients[clientID] = true
		th.reapFinishedClients(false)
		return th.taskExecutor.HandleFinishClient(dataEnvelope, ackHandler)
	}

	if dataEnvelope.GetTotalMessages() != 0 {
		th.totalMessagesToReceive[clientID] = dataEnvelope.GetTotalMessages()
	}
	ackHandler(true, false)
	return nil
}

func (th *taskHandler) Close() error {
	return th.taskExecutor.Close()
}
