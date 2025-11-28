package handler

import (
	"fmt"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

type workerMonitor struct {
	queue           middleware.MessageMiddleware
	startOrFinishCh chan bool
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
	counterCh         chan *protocol.MessageCounter
}

func NewControlHandler(middlewareUrl, clientID string, taskType enum.TaskType) ControlHandler {
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
		counterCh:         make(chan *protocol.MessageCounter, 9999),
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

	h.sendControllerReady()

	return h
}

func (ch *controlHandler) AwaitForWorkers() error {
	logger.Logger.Debugf("Started listening for workers done messages of client %s", ch.clientID)
	currentWorkerType := enum.Gateway
	receivedFromCurrentLayer := 0
	sentFromCurrentLayer := 0

	ch.workersMonitoring[currentWorkerType].startOrFinishCh <- true
	for currentWorkerType != enum.None {
		counter := <-ch.counterCh

		receivedFromCurrentLayer++
		sentFromCurrentLayer += int(counter.GetAmountSent())
		seqNum := counter.GetSequenceNumber()

		if _, ok := ch.sequencesSeen[seqNum]; ok {
			logger.Logger.Debugf("[%s] Duplicate counter message received from %s workers with seq num %d, dropping",
				ch.clientID, currentWorkerType, seqNum)
			continue // Drop duplicated
		} else {
			ch.sequencesSeen[seqNum] = true // Save seq num
		}

		if receivedFromCurrentLayer == ch.messagesSentToNextLayer {
			nextLayer := enum.WorkerType(counter.GetNext())
			logger.Logger.Debugf("[%s] All %d messages received from %s workers, next layer %s with msgs: %d",
				ch.clientID, receivedFromCurrentLayer, currentWorkerType, nextLayer, sentFromCurrentLayer)

			clear(ch.sequencesSeen)
			if currentWorkerType != enum.Gateway {
				ch.workersMonitoring[currentWorkerType].startOrFinishCh <- false // finish current layer
			}
			if nextLayer != enum.None && nextLayer != enum.AggregatorWorker {
				ch.workersMonitoring[nextLayer].startOrFinishCh <- true // start next layer
			} else if nextLayer == enum.AggregatorWorker {
				ch.SendDone(nextLayer, sentFromCurrentLayer) // Notify aggregators the total msgs to wait
			}
			currentWorkerType = nextLayer
			ch.messagesSentToNextLayer = sentFromCurrentLayer
			sentFromCurrentLayer = 0
			receivedFromCurrentLayer = 0
			logger.Logger.Debugf("[%s] Proceeding to wait for %s workers", ch.clientID, currentWorkerType)
		}
	}

	select {
	case <-ch.counterCh: // Only open routine is that of gateway
		logger.Logger.Debugf("[%s] Final counter received from Gateway workers, data done", ch.clientID)
	case <-time.After(10 * time.Second):
		logger.Logger.Warnf("[%s] Timeout waiting for final counter from Gateway workers", ch.clientID)
	}
	ch.workersMonitoring[enum.Gateway].startOrFinishCh <- false // Finally finish gateway layer

	logger.Logger.Debugf("[%s] All workers done, proceed with finish sequence", ch.clientID)

	return nil
}

func (ch *controlHandler) SendDone(worker enum.WorkerType, totalMsgs int) error {
	doneMessage := &protocol.DataEnvelope{
		ClientId:      ch.clientID,
		TaskType:      int32(ch.taskType),
		IsDone:        true,
		TotalMessages: int32(totalMsgs),
		Payload:       nil,
	}
	dataBytes, err := proto.Marshal(doneMessage)
	if err != nil {
		logger.Logger.Error("Error marshaling done message:", err)
		return err
	}

	m := middleware.GetFinishExchange(ch.middlewareUrl, []string{string(worker)})
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
		monitor.queue.Close()
	}
}

/* --- UTIL PRIVATE METHODS --- */

func (ch *controlHandler) sendControllerReady() {
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

			ch.counterCh <- counter

			msg.Ack(false)
		}
		doneCh <- true
	})
	<-doneCh
	if e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("[%s] Error starting counter listener: %d", ch.clientID, e)
	}

	logger.Logger.Debugf("[%s] Counter listener for %s stopped", ch.clientID, workerRoute)
}
