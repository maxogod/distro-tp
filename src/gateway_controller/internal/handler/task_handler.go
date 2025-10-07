package handler

import (
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

// TODO: private & task handler per client, bc queues cant start consuming more than once on same instance
type TaskHandler struct {
	// Data forwarding middlewares
	filtersQueueMiddleware middleware.MessageMiddleware
	joinerRefExchange      middleware.MessageMiddleware

	// Node connections middleware
	joinerFinishExchange         middleware.MessageMiddleware
	aggregatorFinishExchange     middleware.MessageMiddleware
	processedDataQueueMiddleware middleware.MessageMiddleware
}

func NewTaskHandler(url string) Handler {
	return &TaskHandler{
		filtersQueueMiddleware: middleware.GetFilterQueue(url),
		joinerRefExchange:      middleware.GetRefDataExchange(url),

		joinerFinishExchange:         middleware.GetFinishExchange(url, []string{string(enum.JoinerWorker)}),
		aggregatorFinishExchange:     middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)}),
		processedDataQueueMiddleware: middleware.GetProcessedDataQueue(url),
	}
}

func (th *TaskHandler) ForwardData(dataBatch *protocol.DataEnvelope, clientID string) error {
	dataBatch.ClientId = clientID
	return nil
}

func (th *TaskHandler) ForwardReferenceData(dataBatch *protocol.DataEnvelope, clientID string) error {
	dataBatch.ClientId = clientID
	return nil
}

func (th *TaskHandler) SendDone(worker enum.WorkerType, clientID string) error {
	log.Debug("Sending done signal to workers")
	return nil
}

func (th *TaskHandler) GetReportData(data chan *protocol.DataEnvelope, clientID string) {
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
					log.Error("Error unmarshaling processed data:", err)
					msg.Nack(false, true)
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

func (th *TaskHandler) Close() {
	th.filtersQueueMiddleware.Close()
	th.joinerRefExchange.Close()
	th.joinerFinishExchange.Close()
	th.aggregatorFinishExchange.Close()
	th.processedDataQueueMiddleware.Close()
}
