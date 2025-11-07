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

type messageHandler struct {
	clientID string

	// Data forwarding middlewares
	messagesSentToNextLayer int
	filtersQueueMiddleware  middleware.MessageMiddleware
	joinerRefExchange       middleware.MessageMiddleware

	// Processed data receiving
	processedDataExchangeMiddleware middleware.MessageMiddleware
	processedCh                     chan *protocol.DataEnvelope

	// Controller middleware
	initControlQueue     middleware.MessageMiddleware
	controlReadyExchange middleware.MessageMiddleware
	counterExchange      middleware.MessageMiddleware

	startAwaitingAck chan bool
	ackReceived      chan bool
	routineReadyCh   chan bool
	receivingTimeout time.Duration
}

func NewMessageHandler(middlewareUrl, clientID string, receivingTimeout int) MessageHandler {
	h := &messageHandler{
		clientID: clientID,

		filtersQueueMiddleware: middleware.GetFilterQueue(middlewareUrl),
		joinerRefExchange:      middleware.GetRefDataExchange(middlewareUrl),

		processedDataExchangeMiddleware: middleware.GetProcessedDataExchange(middlewareUrl, clientID),
		processedCh:                     make(chan *protocol.DataEnvelope, 9999),

		initControlQueue:     middleware.GetInitControlQueue(middlewareUrl),
		controlReadyExchange: middleware.GetClientControlExchange(middlewareUrl, clientID),
		counterExchange:      middleware.GetCounterExchange(middlewareUrl, clientID+"@"+string(enum.Gateway)),

		startAwaitingAck: make(chan bool),
		ackReceived:      make(chan bool),
		routineReadyCh:   make(chan bool),
		receivingTimeout: time.Duration(receivingTimeout) * time.Second,
	}

	go h.startReportDataListener()
	<-h.routineReadyCh

	go h.awaitControllerAckListener()
	<-h.routineReadyCh

	return h
}

func (mh *messageHandler) AwaitControllerInit() error {
	// Send init message to controller
	controlMessage := &protocol.ControlMessage{
		ClientId: mh.clientID,
	}
	payload, err := proto.Marshal(controlMessage)
	if err != nil {
		return err
	}
	if err := mh.initControlQueue.Send(payload); err != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error sending init control message to controller")
	}

	// Await ack from controller
	mh.startAwaitingAck <- true
	ackReceived := <-mh.ackReceived
	if !ackReceived {
		return fmt.Errorf("did not receive ack from controller")
	}

	return nil
}

func (mh *messageHandler) NotifyClientMessagesCount() error {
	countMessage := &protocol.MessageCounter{
		ClientId:   mh.clientID,
		From:       string(enum.Gateway),
		Next:       string(enum.FilterWorker),
		AmountSent: int32(mh.messagesSentToNextLayer),
	}
	payload, err := proto.Marshal(countMessage)
	if err != nil {
		return err
	}
	if err := mh.counterExchange.Send(payload); err != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error sending messages count to controller")
	}
	return nil
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

func (mh *messageHandler) GetReportData(data chan *protocol.DataEnvelope) {
	for envelope := range mh.processedCh {
		data <- envelope
	}
	close(data)
}

func (mh *messageHandler) Close() {
	mh.filtersQueueMiddleware.Close()
	mh.joinerRefExchange.Close()
	mh.processedDataExchangeMiddleware.Close()
}

/* --- UTIL PRIVATE METHODS --- */

// startReportDataListener starts a go routine that consumes msgs from the process data queue.
func (mh *messageHandler) startReportDataListener() {
	defer mh.processedDataExchangeMiddleware.StopConsuming()

	done := make(chan bool)
	mh.processedDataExchangeMiddleware.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		log.Debugf("[%s] Started listening for processed data", mh.clientID)
		mh.routineReadyCh <- true
		receiving := true
		firstMessageReceived := false // To track if aggregator has started sending data

		timer := time.NewTimer(mh.receivingTimeout)
		defer timer.Stop()
		defer close(mh.processedCh)

		for receiving {
			select {
			case msg := <-msgs:
				// Reset timer
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(mh.receivingTimeout)

				envelope := &protocol.DataEnvelope{}
				err := proto.Unmarshal(msg.Body, envelope)
				if err != nil || envelope.GetClientId() != mh.clientID {
					msg.Nack(false, false) // Discard unwanted messages
					continue
				}

				mh.processedCh <- envelope
				msg.Ack(false)
				if envelope.GetIsDone() && enum.TaskType(envelope.GetTaskType()) != enum.T2_1 {
					receiving = false
				} else if !firstMessageReceived {
					firstMessageReceived = true
				}
			case <-timer.C:
				// Only stop receiving if at least one message was received before
				if firstMessageReceived {
					log.Warnf("[%s] Timeout waiting for processed data", mh.clientID)
					receiving = false
				}
				timer.Reset(mh.receivingTimeout)
			}
		}
		log.Debugf("[%s] Finished listening for processed data", mh.clientID)
		done <- true
	})
	<-done
}

func (mh *messageHandler) awaitControllerAckListener() {
	defer mh.controlReadyExchange.StopConsuming()

	done := make(chan bool)
	mh.controlReadyExchange.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		log.Debugf("[%s] Started listening for controller ack", mh.clientID)
		mh.routineReadyCh <- true
		<-mh.startAwaitingAck
		waiting := true
		ackReceived := false

		for waiting {
			select {
			case msg := <-msgs:
				controlMessage := &protocol.ControlMessage{}
				err := proto.Unmarshal(msg.Body, controlMessage)
				if err != nil || controlMessage.GetClientId() != mh.clientID {
					msg.Nack(false, false) // Discard unwanted messages
					log.Warnf("[%s] Received invalid control message while waiting for ack", mh.clientID)
					continue
				}

				if controlMessage.GetIsAck() {
					waiting = false
					ackReceived = true
					log.Infof("[%s] Received controller ack for initialization", mh.clientID)
				}
				msg.Ack(false)
			case <-time.After(mh.receivingTimeout):
				waiting = false
				ackReceived = false
				log.Warnf("[%s] Timeout waiting for controller ack after %v", mh.clientID, mh.receivingTimeout)
			}
		}

		mh.ackReceived <- ackReceived
		done <- true
	})
	<-done
}
