package handler

import (
	"crypto/cipher"
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
	filtersQueueMiddleware middleware.MessageMiddleware
	joinerRefExchange      middleware.MessageMiddleware

	// Node connections middleware
	messagesSentToNextLayer         int
	counterExchange                 middleware.MessageMiddleware
	joinerFinishExchange            middleware.MessageMiddleware
	aggregatorFinishExchange        middleware.MessageMiddleware
	processedDataExchangeMiddleware middleware.MessageMiddleware
}

func NewMessageHandler(middlewareUrl, clientID string) MessageHandler {
	return &messageHandler{
		clientID: clientID,

		filtersQueueMiddleware: middleware.GetFilterQueue(middlewareUrl),
		joinerRefExchange:      middleware.GetRefDataExchange(middlewareUrl),

		counterExchange:                 middleware.GetCounterExchange(middlewareUrl, clientID),
		joinerFinishExchange:            middleware.GetFinishExchange(middlewareUrl, []string{string(enum.JoinerWorker)}),
		aggregatorFinishExchange:        middleware.GetFinishExchange(middlewareUrl, []string{string(enum.AggregatorWorker)}),
		processedDataExchangeMiddleware: middleware.GetProcessedDataExchange(middlewareUrl, clientID),
	}
}

func (th *messageHandler) ForwardData(dataBatch *protocol.DataEnvelope) error {
	dataBatch.ClientId = th.clientID

	dateBytes, err := proto.Marshal(dataBatch)
	if err != nil {
		log.Error("Error marshaling data batch:", err)
		return err
	}

	if sendErr := th.filtersQueueMiddleware.Send(dateBytes); sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error sending data batch to filters queue")
	}
	th.messagesSentToNextLayer++
	return nil
}

func (th *messageHandler) ForwardReferenceData(dataBatch *protocol.DataEnvelope) error {
	dataBatch.ClientId = th.clientID

	dateBytes, err := proto.Marshal(dataBatch)
	if err != nil {
		log.Error("Error marshaling reference data batch:", err)
		return err
	}

	if sendErr := th.joinerRefExchange.Send(dateBytes); sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error sending reference data batch to joiner exchange")
	}
	return nil
}

func (th *messageHandler) AwaitForWorkers() error {
	defer th.counterExchange.StopConsuming()

	doneCh := make(chan bool)
	e := th.counterExchange.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		currentWorkerType := enum.FilterWorker
		receivedFromCurrentLayer := 0
		sentFromCurrentLayer := 0

		waiting := true
		for waiting {
			msg, _ := <-msgs
			counter := &protocol.MessageCounter{}
			err := proto.Unmarshal(msg.Body, counter)
			if err != nil ||
				counter.GetClientId() != th.clientID ||
				enum.WorkerType(counter.GetFrom()) != currentWorkerType {
				msg.Nack(false, false)
			}

			receivedFromCurrentLayer++
			sentFromCurrentLayer += int(counter.GetAmountSent())

			if receivedFromCurrentLayer == th.messagesSentToNextLayer {
				log.Debugf("All done messages received from %s workers", currentWorkerType)
				currentWorkerType = enum.WorkerType(counter.GetNext())
				th.messagesSentToNextLayer = sentFromCurrentLayer
				sentFromCurrentLayer = 0
				receivedFromCurrentLayer = 0
			}

			if currentWorkerType == enum.AggregatorWorker {
				log.Debug("All done messages received from worker layers and sent to aggregator")
				waiting = false
			}

			msg.Ack(false)
		}
		doneCh <- true
	})
	<-doneCh

	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error while awaiting for workers done messages")
	}

	return nil
}

func (th *messageHandler) SendDone(worker enum.WorkerType) error {
	messageCounter := &protocol.MessageCounter{
		ClientId:   th.clientID,
		AmountSent: int32(th.messagesSentToNextLayer),
	}
	counterBytes, err := proto.Marshal(messageCounter)
	if err != nil {
		log.Error("Error marshaling message counter:", err)
		return err
	}

	doneMessage := &protocol.DataEnvelope{
		ClientId: th.clientID,
		IsDone:   true,
		Payload:  counterBytes,
	}
	dataBytes, err := proto.Marshal(doneMessage)
	if err != nil {
		log.Error("Error marshaling done message:", err)
		return err
	}

	var sendErr middleware.MessageMiddlewareError
	switch worker {
	case enum.JoinerWorker:
		sendErr = th.joinerFinishExchange.Send(dataBytes)
	case enum.AggregatorWorker:
		sendErr = th.aggregatorFinishExchange.Send(dataBytes)
	default:
		return fmt.Errorf("unknown worker type: %s", worker)
	}

	if sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error sending done message to %s exchange", worker)
	}
	return nil
}

func (th *messageHandler) GetReportData(data chan *protocol.DataEnvelope) {
	defer th.processedDataExchangeMiddleware.StopConsuming()

	done := make(chan bool)
	th.processedDataExchangeMiddleware.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
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
				if err != nil || envelope.GetClientId() != th.clientID {
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

func (th *messageHandler) Close() {
	th.filtersQueueMiddleware.Close()
	th.joinerRefExchange.Close()
	th.joinerFinishExchange.Close()
	th.aggregatorFinishExchange.Close()
	th.processedDataExchangeMiddleware.Close()
	th.counterExchange.Close()
}
