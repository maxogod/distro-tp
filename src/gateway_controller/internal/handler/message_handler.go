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

var log = logger.GetLogger()

type workerMonitor struct {
	queue           middleware.MessageMiddleware
	startOrFinishCh chan bool
}

type messageHandler struct {
	clientID string

	// Data forwarding middlewares
	filtersQueueMiddleware middleware.MessageMiddleware
	joinerRefExchange      middleware.MessageMiddleware

	// Node connections middleware
	messagesSentToNextLayer         int
	joinerFinishExchange            middleware.MessageMiddleware
	aggregatorFinishExchange        middleware.MessageMiddleware
	processedDataExchangeMiddleware middleware.MessageMiddleware

	workersMonitoring map[enum.WorkerType]workerMonitor
	counterCh         chan *protocol.MessageCounter
}

func NewMessageHandler(middlewareUrl, clientID string) MessageHandler {
	h := &messageHandler{
		clientID: clientID,

		filtersQueueMiddleware: middleware.GetFilterQueue(middlewareUrl),
		joinerRefExchange:      middleware.GetRefDataExchange(middlewareUrl),

		joinerFinishExchange:            middleware.GetFinishExchange(middlewareUrl, []string{string(enum.JoinerWorker)}),
		aggregatorFinishExchange:        middleware.GetFinishExchange(middlewareUrl, []string{string(enum.AggregatorWorker)}),
		processedDataExchangeMiddleware: middleware.GetProcessedDataExchange(middlewareUrl, clientID),

		workersMonitoring: make(map[enum.WorkerType]workerMonitor),
		counterCh:         make(chan *protocol.MessageCounter, 9999),
	}

	workers := []enum.WorkerType{
		enum.FilterWorker, enum.GroupbyWorker, enum.ReducerWorker,
		enum.JoinerWorker, enum.AggregatorWorker,
	}
	for _, worker := range workers {
		h.workersMonitoring[worker] = workerMonitor{
			queue:           middleware.GetCounterExchange(middlewareUrl, clientID+"@"+string(worker)),
			startOrFinishCh: make(chan bool),
		}
		go h.startCounterListener(worker)
	}

	return h
}

func (mh *messageHandler) ForwardData(dataBatch *protocol.DataEnvelope) error {
	dataBatch.ClientId = mh.clientID

	dateBytes, err := proto.Marshal(dataBatch)
	if err != nil {
		log.Error("Error marshaling data batch:", err)
		return err
	}

	if sendErr := mh.filtersQueueMiddleware.Send(dateBytes); sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error sending data batch to filters queue")
	}
	mh.messagesSentToNextLayer++
	return nil
}

func (mh *messageHandler) ForwardReferenceData(dataBatch *protocol.DataEnvelope) error {
	dataBatch.ClientId = mh.clientID

	dateBytes, err := proto.Marshal(dataBatch)
	if err != nil {
		log.Error("Error marshaling reference data batch:", err)
		return err
	}

	if sendErr := mh.joinerRefExchange.Send(dateBytes); sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error sending reference data batch to joiner exchange")
	}
	return nil
}

func (mh *messageHandler) AwaitForWorkers() error {
	log.Debugf("Started listening for workers done messages of client %s", mh.clientID)
	currentWorkerType := enum.FilterWorker
	receivedFromCurrentLayer := 0
	sentFromCurrentLayer := 0

	mh.workersMonitoring[currentWorkerType].startOrFinishCh <- true
	for currentWorkerType != enum.None {
		counter := <-mh.counterCh

		receivedFromCurrentLayer++
		sentFromCurrentLayer += int(counter.GetAmountSent())

		if receivedFromCurrentLayer == mh.messagesSentToNextLayer {
			nextLayer := enum.WorkerType(counter.GetNext())
			log.Debugf("All %d messages received from %s workers, next layer %s with msgs: %d",
				receivedFromCurrentLayer, currentWorkerType, nextLayer, sentFromCurrentLayer)

			mh.workersMonitoring[currentWorkerType].startOrFinishCh <- true // finish
			if nextLayer != enum.None {
				mh.workersMonitoring[nextLayer].startOrFinishCh <- true // start
			}
			currentWorkerType = nextLayer
			mh.messagesSentToNextLayer = sentFromCurrentLayer
			sentFromCurrentLayer = 0
			receivedFromCurrentLayer = 0
			log.Debugf("Proceeding to wait for %s workers", currentWorkerType)
		}
	}
	log.Debug("All workers done, proceed with finish sequence")

	return nil
}

func (mh *messageHandler) SendDone(worker enum.WorkerType) error {
	doneMessage := &protocol.DataEnvelope{
		ClientId: mh.clientID,
		IsDone:   true,
		Payload:  nil,
	}
	dataBytes, err := proto.Marshal(doneMessage)
	if err != nil {
		log.Error("Error marshaling done message:", err)
		return err
	}

	var sendErr middleware.MessageMiddlewareError
	switch worker {
	case enum.JoinerWorker:
		sendErr = mh.joinerFinishExchange.Send(dataBytes)
	case enum.AggregatorWorker:
		sendErr = mh.aggregatorFinishExchange.Send(dataBytes)
	default:
		return fmt.Errorf("unknown worker type: %s", worker)
	}

	if sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error sending done message to %s exchange", worker)
	}
	return nil
}

func (mh *messageHandler) GetReportData(data chan *protocol.DataEnvelope) {
	defer mh.processedDataExchangeMiddleware.StopConsuming()

	done := make(chan bool)
	mh.processedDataExchangeMiddleware.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		log.Debug("Started listening for processed data")
		receiving := true
		firstMessageReceived := false // To track if aggregator has started sending data

		timer := time.NewTimer(RECEIVING_TIMEOUT)
		defer timer.Stop()
		defer close(data)

		for receiving {
			select {
			case msg := <-msgs:
				// Reset timer
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(RECEIVING_TIMEOUT)

				envelope := &protocol.DataEnvelope{}
				err := proto.Unmarshal(msg.Body, envelope)
				if err != nil || envelope.GetClientId() != mh.clientID {
					msg.Nack(false, false) // Discard unwanted messages
					continue
				}

				data <- envelope
				msg.Ack(false)
				if envelope.GetIsDone() && enum.TaskType(envelope.GetTaskType()) != enum.T2_1 {
					receiving = false
				} else if !firstMessageReceived {
					firstMessageReceived = true
				}
			case <-timer.C:
				// Only stop receiving if at least one message was received before
				if firstMessageReceived {
					log.Warnln("Timeout waiting for processed data")
					receiving = false
				}
				timer.Reset(RECEIVING_TIMEOUT)
			}
		}
		log.Debug("Finished listening for processed data")
		done <- true
	})
	<-done
}

func (mh *messageHandler) Close() {
	mh.filtersQueueMiddleware.Close()
	mh.joinerRefExchange.Close()
	mh.joinerFinishExchange.Close()
	mh.aggregatorFinishExchange.Close()
	mh.processedDataExchangeMiddleware.Close()
	for _, monitor := range mh.workersMonitoring {
		monitor.queue.Close()
	}
}

/* --- UTIL PRIVATE METHODS --- */

// startCounterListener starts a goroutine that listens for counter messages from workers.
func (mh *messageHandler) startCounterListener(workerRoute enum.WorkerType) {
	defer mh.workersMonitoring[workerRoute].queue.StopConsuming()

	doneCh := make(chan bool)
	e := mh.workersMonitoring[workerRoute].queue.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		<-mh.workersMonitoring[workerRoute].startOrFinishCh
		log.Debugf("Starting to consume from counter exchange for %s workers", workerRoute)
		running := true
		for running {
			var msg middleware.MessageDelivery
			select {
			case m := <-msgs:
				msg = m
			case <-mh.workersMonitoring[workerRoute].startOrFinishCh:
				running = false
				continue
			}

			counter := &protocol.MessageCounter{}
			err := proto.Unmarshal(msg.Body, counter)
			if err != nil {
				log.Errorf("Failed to unmarshal done message: %v", err)
				msg.Nack(false, false)
				continue
			}
			if counter.GetClientId() != mh.clientID || enum.WorkerType(counter.GetFrom()) != workerRoute {
				log.Warnf("Received wrong clientID %s or WorkerType %s", counter.GetClientId(), counter.GetFrom())
				msg.Nack(false, false)
				continue
			}

			mh.counterCh <- counter

			msg.Ack(false)
		}
		doneCh <- true
	})
	<-doneCh
	if e != middleware.MessageMiddlewareSuccess {
		log.Error("Error starting counter listener:", e)
	}

	log.Debugf("Counter listener for %s stopped", workerRoute)
}
