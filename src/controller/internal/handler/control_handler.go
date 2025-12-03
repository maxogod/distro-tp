package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/controller/internal/storage"
	"google.golang.org/protobuf/proto"
)

type workerMonitor struct {
	queue           middleware.MessageMiddleware
	startOrFinishCh chan bool
}

type counterMessage struct {
	counter    *protocol.MessageCounter
	ackHandler func(bool, bool) error
	persisted  bool
}

type controlHandler struct {
	clientID      string
	taskType      enum.TaskType
	middlewareUrl string
	sequencesSeen map[int32]bool

	// Node connections middleware
	messagesSentToNextLayer int
	filterQueue             middleware.MessageMiddleware
	clientControlExchange   middleware.MessageMiddleware

	workersMonitoring map[enum.WorkerType]workerMonitor
	routineReadyCh    chan bool
	counterCh         chan counterMessage

	counterStore      storage.CounterStorage
	preloadedCounters []*protocol.MessageCounter
}

func NewControlHandler(
	middlewareUrl, clientID string,
	taskType enum.TaskType,
	counterStore storage.CounterStorage,
	storedCounters []*protocol.MessageCounter,
) ControlHandler {
	h := &controlHandler{
		clientID:      clientID,
		taskType:      taskType,
		middlewareUrl: middlewareUrl,
		sequencesSeen: make(map[int32]bool),

		messagesSentToNextLayer: 1, // start with 1 message from gateway
		filterQueue:             middleware.GetFilterQueue(middlewareUrl),
		clientControlExchange:   middleware.GetClientControlExchange(middlewareUrl, clientID),

		workersMonitoring: make(map[enum.WorkerType]workerMonitor),
		routineReadyCh:    make(chan bool),
		counterCh:         make(chan counterMessage, 9999),

		counterStore:      counterStore,
		preloadedCounters: storedCounters,
	}

	workers := []enum.WorkerType{
		enum.Gateway, enum.FilterWorker,
	}
	for _, worker := range workers {
		h.workersMonitoring[worker] = workerMonitor{
			queue:           middleware.GetCounterExchange(middlewareUrl, clientID+"@"+string(worker)),
			startOrFinishCh: make(chan bool, 2),
		}
		go h.startCounterListener(worker)
		<-h.routineReadyCh
	}

	h.SendControllerReady()

	return h
}

func (ch *controlHandler) AwaitForWorkers() error {
	logger.Logger.Debugf("Started listening for workers done messages of client %s", ch.clientID)
	currentWorkerType := enum.Gateway
	receivedFromCurrentLayer := 0
	sentFromCurrentLayer := 0

	go ch.enqueueStoredCounters()

	logger.Logger.Debugf("[%s] Starting to wait for %s workers", ch.clientID, currentWorkerType)

	ch.workersMonitoring[currentWorkerType].startOrFinishCh <- true
	for currentWorkerType != enum.None && currentWorkerType != enum.AggregatorWorker {

		counterMsg := <-ch.counterCh
		counter := counterMsg.counter

		if enum.WorkerType(counter.GetFrom()) == enum.Gateway && enum.WorkerType(counter.GetNext()) == enum.Controller {
			logger.Logger.Debugf("[%s] Receive Gateway abort for client %s", ch.clientID, ch.clientID)
			ch.workersMonitoring[enum.Gateway].startOrFinishCh <- false      // Finally finish gateway layer
			ch.workersMonitoring[enum.FilterWorker].startOrFinishCh <- false // Finally finish filter layer

			err := ch.counterStore.RemoveClient(ch.clientID)
			if err != nil {
				logger.Logger.Errorf("[%s] Error removing client from storage: %v", ch.clientID, err)
			}

			if counterMsg.ackHandler != nil {
				counterMsg.ackHandler(true, false)
			}
			return nil
		}

		seqNum := counter.GetSequenceNumber()

		if _, ok := ch.sequencesSeen[seqNum]; ok {
			logger.Logger.Debugf("[%s] Duplicate counter message received from %s workers with seq num %d, dropping",
				ch.clientID, currentWorkerType, seqNum)
			if counterMsg.ackHandler != nil {
				counterMsg.ackHandler(true, false)
			}
			continue // Drop duplicated
		}

		if !counterMsg.persisted {
			err := ch.counterStore.AppendCounter(ch.clientID, counter)
			if err != nil {
				logger.Logger.Errorf("[%s] failed to persist counter: %v. Requeuing message", ch.clientID, err)
				counterBytes, _ := proto.Marshal(counterMsg.counter)
				ch.workersMonitoring[currentWorkerType].queue.Send(counterBytes)
				if counterMsg.ackHandler != nil {
					counterMsg.ackHandler(true, false)
				}
				continue
			}
		}

		receivedFromCurrentLayer++
		sentFromCurrentLayer += int(counter.GetAmountSent())
		ch.sequencesSeen[seqNum] = true // Save seq num

		if receivedFromCurrentLayer == ch.messagesSentToNextLayer {
			nextLayer := enum.WorkerType(counter.GetNext())
			logger.Logger.Debugf("[%s] All %d messages received from %s workers, next layer %s with msgs: %d",
				ch.clientID, receivedFromCurrentLayer, currentWorkerType, nextLayer, sentFromCurrentLayer)

			clear(ch.sequencesSeen)
			if currentWorkerType != enum.Gateway { // Gateways routine will be used later
				ch.workersMonitoring[currentWorkerType].startOrFinishCh <- false // finish current layer
			}
			if nextLayer != enum.None && nextLayer != enum.AggregatorWorker {
				ch.workersMonitoring[nextLayer].startOrFinishCh <- true // start next layer
			} else if nextLayer == enum.AggregatorWorker {
				ch.SendDone(nextLayer, sentFromCurrentLayer, false) // Notify aggregators the total msgs to wait
			} else if nextLayer == enum.None && currentWorkerType == enum.FilterWorker {
				logger.Logger.Debugf("[%s] No more layers after Filter, sending done to Filter workers", ch.clientID)
				ch.SendDone(currentWorkerType, 0, false)
			}
			currentWorkerType = nextLayer
			ch.messagesSentToNextLayer = sentFromCurrentLayer
			sentFromCurrentLayer = 0
			receivedFromCurrentLayer = 0
			logger.Logger.Debugf("[%s] Proceeding to wait for %s workers", ch.clientID, currentWorkerType)
		}
		if counterMsg.ackHandler != nil {
			counterMsg.ackHandler(true, false)
		}
	}

	<-ch.counterCh
	logger.Logger.Debugf("[%s] Final counter received from Gateway workers, data done", ch.clientID)

	clientQueue := middleware.GetProcessedDataExchange(ch.middlewareUrl, ch.clientID)
	defer clientQueue.Close()
	worker.SendDone(ch.clientID, ch.taskType, clientQueue)
	ch.workersMonitoring[enum.Gateway].startOrFinishCh <- false // Finally finish gateway layer

	logger.Logger.Debugf("[%s] All workers done, proceed with finish sequence", ch.clientID)

	return nil
}

func (ch *controlHandler) SendDone(worker enum.WorkerType, totalMsgs int, deleteAction bool) error {
	seq := 0
	if deleteAction {
		seq = -1
	}
	doneMessage := &protocol.DataEnvelope{
		ClientId:       ch.clientID,
		TaskType:       int32(ch.taskType),
		IsDone:         true,
		TotalMessages:  int32(totalMsgs),
		SequenceNumber: int32(seq),
		Payload:        nil,
	}
	dataBytes, err := proto.Marshal(doneMessage)
	if err != nil {
		logger.Logger.Error("Error marshaling done message:", err)
		return err
	}

	m := middleware.GetFinishExchange(ch.middlewareUrl, []string{string(worker)}, "")
	defer m.Close()

	sendErr := m.Send(dataBytes)
	if sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error sending done message to %s exchange", worker)
	}
	return nil
}

func (ch *controlHandler) Close() {
	for _, monitor := range ch.workersMonitoring {
		monitor.startOrFinishCh <- false
		monitor.queue.Delete()
		monitor.queue.Close()
	}
}

func (ch *controlHandler) CleanupStorage() {
	logger.Logger.Debugf("[%s] Cleaning up storage", ch.clientID)
	if ch.counterStore != nil {
		_ = ch.counterStore.RemoveClient(ch.clientID)
	}
}

func (ch *controlHandler) SendControllerReady() {
	readyMessage := &protocol.ControlMessage{
		ClientId: ch.clientID,
		IsAck:    true,
	}
	dataBytes, err := proto.Marshal(readyMessage)
	if err != nil {
		logger.Logger.Errorf("[%s] Error marshaling controller ready message: %v", ch.clientID, err)
		return
	}

	sendErr := ch.clientControlExchange.Send(dataBytes)
	if sendErr != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("[%s] Error sending controller ready message: %d", ch.clientID, sendErr)
		return
	}

	logger.Logger.Infof("[%s] Controller ready message sent", ch.clientID)
}

/* --- UTIL PRIVATE METHODS --- */

func (ch *controlHandler) enqueueStoredCounters() {
	logger.Logger.Debugf("[%s] Enqueuing %d stored counters", ch.clientID, len(ch.preloadedCounters))
	for _, counter := range ch.preloadedCounters {
		ch.counterCh <- counterMessage{
			counter:    counter,
			ackHandler: nil,
			persisted:  true,
		}
	}
	ch.preloadedCounters = nil
}

// startCounterListener starts a goroutine that listens for counter messages from workers.
func (ch *controlHandler) startCounterListener(workerRoute enum.WorkerType) {
	defer ch.workersMonitoring[workerRoute].queue.StopConsuming()

	doneCh := make(chan bool)
	e := ch.workersMonitoring[workerRoute].queue.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		logger.Logger.Debugf("[%s] Starting to consume from counter exchange for %s workers", ch.clientID, workerRoute)
		ch.routineReadyCh <- true
		running := <-ch.workersMonitoring[workerRoute].startOrFinishCh
		for running {
			var msg middleware.MessageDelivery
			select {
			case m := <-msgs:
				msg = m
			case <-ch.workersMonitoring[workerRoute].startOrFinishCh:
				running = false
				continue
			}

			counter := &protocol.MessageCounter{}
			err := proto.Unmarshal(msg.Body, counter)
			if err != nil {
				logger.Logger.Errorf("[%s] Failed to unmarshal done message: %v", ch.clientID, err)
				msg.Nack(false, false)
				continue
			}
			if counter.GetClientId() != ch.clientID || enum.WorkerType(counter.GetFrom()) != workerRoute {
				logger.Logger.Warnf("[%s] Received wrong clientID or WorkerType %s", counter.GetClientId(), counter.GetFrom())
				msg.Nack(false, false)
				continue
			}

			newCounterMessage := counterMessage{
				counter:    counter,
				ackHandler: ackHandler(msg),
			}

			ch.counterCh <- newCounterMessage
		}
		doneCh <- true
	})
	<-doneCh
	if e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("[%s] Error starting counter listener: %d", ch.clientID, e)
	}

	logger.Logger.Debugf("[%s] Counter listener for %s stopped", ch.clientID, workerRoute)
}

// ackHandler returns a function that can be used to ack/nack a message.
func ackHandler(msg middleware.MessageDelivery) func(bool, bool) error {
	return func(ack, requeue bool) error {
		if ack {
			return msg.Ack(false)
		}
		return msg.Nack(false, requeue)
	}
}
