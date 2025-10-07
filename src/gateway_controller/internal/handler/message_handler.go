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
	filtersQueueMiddleware middleware.MessageMiddleware
	joinerRefExchange      middleware.MessageMiddleware

	// Node connections middleware
	joinerFinishExchange         middleware.MessageMiddleware
	aggregatorFinishExchange     middleware.MessageMiddleware
	processedDataQueueMiddleware middleware.MessageMiddleware
}

func NewMessageHandler(middlewareUrl, clientID string) MessageHandler {
	return &messageHandler{
		clientID: clientID,

		filtersQueueMiddleware: middleware.GetFilterQueue(middlewareUrl),
		joinerRefExchange:      middleware.GetRefDataExchange(middlewareUrl),

		joinerFinishExchange:         middleware.GetFinishExchange(middlewareUrl, []string{string(enum.JoinerWorker)}),
		aggregatorFinishExchange:     middleware.GetFinishExchange(middlewareUrl, []string{string(enum.AggregatorWorker)}),
		processedDataQueueMiddleware: middleware.GetProcessedDataQueue(middlewareUrl),
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

func (th *messageHandler) SendDone(worker enum.WorkerType) error {
	doneMessage := &protocol.DataEnvelope{
		ClientId: th.clientID,
		IsDone:   true,
	}

	dateBytes, err := proto.Marshal(doneMessage)
	if err != nil {
		log.Error("Error marshaling done message:", err)
		return err
	}

	var sendErr middleware.MessageMiddlewareError
	switch worker {
	case enum.JoinerWorker:
		sendErr = th.joinerFinishExchange.Send(dateBytes)
	case enum.AggregatorWorker:
		sendErr = th.aggregatorFinishExchange.Send(dateBytes)
	default:
		return fmt.Errorf("unknown worker type: %s", worker)
	}

	if sendErr != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("error sending done message to %s exchange", worker)
	}
	return nil
}

func (th *messageHandler) GetReportData(data chan *protocol.DataEnvelope) {
	defer th.processedDataQueueMiddleware.StopConsuming()

	done := make(chan bool)
	th.processedDataQueueMiddleware.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		log.Debug("Started listening for processed data")
		receiving := true
		for receiving {
			select {
			case msg := <-msgs:
				envelope := &protocol.DataEnvelope{}
				err := proto.Unmarshal(msg.Body, envelope)
				if err != nil {
					msg.Nack(false, false) // Discard corrupted messages
					continue
				} else if envelope.GetClientId() != th.clientID {
					msg.Nack(false, true) // Requeue other clients messages
					continue
				}

				data <- envelope
				msg.Ack(false)
				if envelope.GetIsDone() {
					receiving = false
				}
			case <-time.After(RECEIVING_TIMEOUT):
				log.Warnln("Timeout waiting for processed data")
				receiving = false
			}
		}
		log.Debug("Finished listening for processed data")
		close(data)
		done <- true
	})
	<-done
}

func (th *messageHandler) Close() {
	th.filtersQueueMiddleware.Close()
	th.joinerRefExchange.Close()
	th.joinerFinishExchange.Close()
	th.aggregatorFinishExchange.Close()
	th.processedDataQueueMiddleware.Close()
}
