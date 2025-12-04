package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
)

const FINISH enum.TaskType = 0

const (
	REAP_AFTER_MSGS = 10000

	STORAGE_FOLDER_PATH        = "storage/"
	SEPARATOR                  = "@"
	TOTAL_COUNT_FILE_EXTENSION = ".count"
)

type clientDiskTuple struct {
	total    int32
	taskType enum.TaskType
}

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
	taskHandlers       map[enum.TaskType]func(*protocol.DataEnvelope, func(bool, bool) error) error
	sequencesPerClient map[string]map[int32]bool
	finishedClients    map[string]bool
	taskExecutor       TaskExecutor

	shouldDropDuplicates bool
	shouldPersistTotals  bool

	messagesReceived       map[string]int32
	totalMessagesToReceive map[string]int32
}

// NewTaskHandler creates a new TaskHandler with the provided TaskExecutor
// If shouldDropDuplicates is true, the handler will track and drop duplicate messages based on sequence numbers.
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

// NewTaskHandlerWithSeqs creates a new TaskHandler with the provided TaskExecutor
// and initializes it with existing sequences per client to track duplicates.
func NewTaskHandlerWithSeqs(taskExecutor TaskExecutor, shouldDropDuplicates bool, seqs map[string][]int32) DataHandler {
	th := NewTaskHandler(taskExecutor, shouldDropDuplicates).(*taskHandler)
	th.shouldPersistTotals = true

	for clientID, seqList := range seqs {
		seqMap := make(map[int32]bool)
		for _, seq := range seqList {
			seqMap[seq] = true
		}
		th.sequencesPerClient[clientID] = seqMap
	}

	totals := th.getTotals()
	if totals == nil {
		return th
	}

	for clientID, client := range totals {
		currentSeqs, ok := th.sequencesPerClient[clientID]
		if !ok {
			// TODO: remove bad total file?
			logger.Logger.Warnf("[%s] No sequences found for client with total file. Ignoring total file.", clientID)
			continue
		}
		th.totalMessagesToReceive[clientID] = client.total
		if int32(len(currentSeqs)) >= client.total {
			done := &protocol.DataEnvelope{
				ClientId: clientID,
				IsDone:   true,
				TaskType: int32(client.taskType),
			}
			logger.Logger.Debugf("[%s] FOund finished client in disk, calling HandleFinishClient", clientID)
			taskExecutor.HandleFinishClient(done, func(bool, bool) error { return nil })
		}
	}

	logger.Logger.Debugf("Finished loading existing clients from disk")

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
		// TODO: what if it dies here?
		if err := th.HandleFinishClient(dataEnvelope, func(bool, bool) error { return nil }); err != nil {
			return err
		}
		if err := th.removeTotalFile(clientID); err != nil {
			logger.Logger.Errorf("[%s] Error removing progress file: %v", clientID, err)
		}
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

	if err := th.createTotalFile(clientID, dataEnvelope.GetTotalMessages(), dataEnvelope.GetTaskType()); err != nil {
		logger.Logger.Errorf("[%s] Error creating progress file: %v", clientID, err)
	}

	if dataEnvelope.GetTotalMessages() == 0 || count == dataEnvelope.GetTotalMessages() {
		logger.Logger.Debugf("[%s] Calling HandleFinishClient", clientID)
		th.finishedClients[clientID] = true
		th.reapFinishedClients(false)
		if err := th.taskExecutor.HandleFinishClient(dataEnvelope, ackHandler); err != nil {
			logger.Logger.Errorf("[%s] Error handling finish for client %v", clientID, err)
			return err
		}
		if err := th.removeTotalFile(clientID); err != nil {
			logger.Logger.Errorf("[%s] Error removing progress file: %v", clientID, err)
		}
		return nil
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

/* --- HELPERS --- */

func (th *taskHandler) createTotalFile(clientID string, total int32, task int32) error {
	if !th.shouldPersistTotals {
		return nil
	}
	logger.Logger.Debugf("[%s] Creating total file with total %d", clientID, total)

	filePath := STORAGE_FOLDER_PATH + clientID + SEPARATOR + strconv.Itoa(int(total)) + SEPARATOR + strconv.Itoa(int(task)) + TOTAL_COUNT_FILE_EXTENSION
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	return file.Close()
}

func (th *taskHandler) removeTotalFile(clientID string) error {
	if !th.shouldPersistTotals {
		return nil
	}

	logger.Logger.Debugf("[%s] Deleting total file", clientID)

	pattern := STORAGE_FOLDER_PATH + clientID + SEPARATOR + "*" + TOTAL_COUNT_FILE_EXTENSION
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func (th *taskHandler) getTotals() map[string]clientDiskTuple {
	references := make(map[string]clientDiskTuple)
	files, err := filepath.Glob(STORAGE_FOLDER_PATH + "*" + TOTAL_COUNT_FILE_EXTENSION)
	if err != nil {
		logger.Logger.Errorf("Error globbing cache files: %v", err)
		return nil
	}
	for _, file := range files {
		reference := file[len(STORAGE_FOLDER_PATH) : len(file)-len(TOTAL_COUNT_FILE_EXTENSION)]
		clientID := strings.Split(reference, SEPARATOR)[0]
		total, err := strconv.Atoi(strings.Split(reference, SEPARATOR)[1])
		if err != nil {
			logger.Logger.Errorf("Error parsing total count from file %s: %v", file, err)
			continue
		}
		taskType, err := strconv.Atoi(strings.Split(reference, SEPARATOR)[2])
		if err != nil {
			logger.Logger.Errorf("Error parsing task type from file %s: %v", file, err)
			continue
		}

		references[clientID] = clientDiskTuple{
			total:    int32(total),
			taskType: enum.TaskType(taskType),
		}
	}
	return references
}
