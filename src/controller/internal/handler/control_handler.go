package handler

import (
	"fmt"

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

type controlHandler struct {
	clientID string

	// Node connections middleware
	messagesSentToNextLayer  int
	joinerFinishExchange     middleware.MessageMiddleware
	aggregatorFinishExchange middleware.MessageMiddleware

	workersMonitoring map[enum.WorkerType]workerMonitor
	routineReadyCh    chan bool
	counterCh         chan *protocol.MessageCounter
}

func NewControlHandler(middlewareUrl, clientID string) MessageHandler {
	h := &controlHandler{
		clientID: clientID,

		joinerFinishExchange:     middleware.GetFinishExchange(middlewareUrl, []string{string(enum.JoinerWorker)}),
		aggregatorFinishExchange: middleware.GetFinishExchange(middlewareUrl, []string{string(enum.AggregatorWorker)}),

		workersMonitoring: make(map[enum.WorkerType]workerMonitor),
		routineReadyCh:    make(chan bool),
		counterCh:         make(chan *protocol.MessageCounter, 9999),
	}

	workers := []enum.WorkerType{
		enum.FilterWorker, enum.GroupbyWorker, enum.ReducerWorker,
		enum.JoinerWorker, enum.AggregatorWorker,
	}
	for _, worker := range workers {
		h.workersMonitoring[worker] = workerMonitor{
			queue:           middleware.GetCounterExchange(middlewareUrl, clientID+"@"+string(worker)),
			startOrFinishCh: make(chan bool, 2),
		}
		go h.startCounterListener(worker)
		<-h.routineReadyCh
	}

	return h
}

func (mh *controlHandler) AwaitForWorkers() error {
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
			log.Debugf("[%s] All %d messages received from %s workers, next layer %s with msgs: %d",
				mh.clientID, receivedFromCurrentLayer, currentWorkerType, nextLayer, sentFromCurrentLayer)

			mh.workersMonitoring[currentWorkerType].startOrFinishCh <- false // finish
			if nextLayer != enum.None {
				mh.workersMonitoring[nextLayer].startOrFinishCh <- true // start
			}
			currentWorkerType = nextLayer
			mh.messagesSentToNextLayer = sentFromCurrentLayer
			sentFromCurrentLayer = 0
			receivedFromCurrentLayer = 0
			log.Debugf("[%s] Proceeding to wait for %s workers", mh.clientID, currentWorkerType)
		}
	}
	log.Debugf("[%s] All workers done, proceed with finish sequence", mh.clientID)

	return nil
}

func (mh *controlHandler) SendDone(worker enum.WorkerType) error {
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

func (mh *controlHandler) Close() {
	mh.joinerFinishExchange.Close()
	mh.aggregatorFinishExchange.Close()
	for _, monitor := range mh.workersMonitoring {
		monitor.startOrFinishCh <- false
		monitor.queue.Close()
	}
}

/* --- UTIL PRIVATE METHODS --- */

// startCounterListener starts a goroutine that listens for counter messages from workers.
func (mh *controlHandler) startCounterListener(workerRoute enum.WorkerType) {
	defer mh.workersMonitoring[workerRoute].queue.StopConsuming()

	doneCh := make(chan bool)
	e := mh.workersMonitoring[workerRoute].queue.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		log.Debugf("[%s] Starting to consume from counter exchange for %s workers", mh.clientID, workerRoute)
		mh.routineReadyCh <- true
		running := <-mh.workersMonitoring[workerRoute].startOrFinishCh
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
				log.Errorf("[%s] Failed to unmarshal done message: %v", mh.clientID, err)
				msg.Nack(false, false)
				continue
			}
			if counter.GetClientId() != mh.clientID || enum.WorkerType(counter.GetFrom()) != workerRoute {
				log.Warnf("[%s] Received wrong clientID or WorkerType %s", counter.GetClientId(), counter.GetFrom())
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
		log.Errorf("[%s] Error starting counter listener: %d", mh.clientID, e)
	}

	log.Debugf("[%s] Counter listener for %s stopped", mh.clientID, workerRoute)
}
