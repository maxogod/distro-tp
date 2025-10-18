package worker

import (
	"fmt"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/utils"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

// when having to handle / finish clients, there is a bool to indicate if
// the client is in finishing mode, and a timer to count down to when it should execute the
// finishClient function
type client struct {
	finishUp bool
	timer    *time.Timer
}

// This struct is required when creating a worker that consumes
// from input queues and produces to output queues
// passing data to a DataHandler function, and sending results to output queues
// It is required to have a DataHandler implementation to handle the data
type messageHandler struct {
	// connections
	inputQueues      []middleware.MessageMiddleware
	finisherExchange middleware.MessageMiddleware

	// internals
	dataHandler     DataHandler
	inputChannel    chan *protocol.DataEnvelope
	finisherChannel chan *protocol.DataEnvelope
	isRunning       bool //GLT
}

func NewMessageHandler(
	dataHandler DataHandler,
	inputQueues []middleware.MessageMiddleware,
	finisherExchange middleware.MessageMiddleware,
) MessageHandler {

	mh := &messageHandler{
		dataHandler:     dataHandler,
		inputQueues:     inputQueues,
		inputChannel:    make(chan *protocol.DataEnvelope),
		finisherChannel: make(chan *protocol.DataEnvelope),
		isRunning:       true,
	}

	// if this requires a finisher queue,
	// then when a finish message is recieved, a timer
	// begins to wrap up any consumption of a specific clients messages
	if finisherExchange != nil {
		mh.finisherExchange = finisherExchange
		log.Debug("Finisher queue consuming.")
		go func() {
			if err := mh.consumeFromQueue(mh.finisherExchange, mh.finisherChannel); err != nil {
				log.Errorf("Failed to consume from finisher queue: %v", err)
			}
		}()
	}

	return mh
}

// Starts consuming indefinetly from monitor channel and handling messages with the provided dataHandler function
func (mh *messageHandler) Start() error {

	log.Debug("Starting MessageHandler...")
	for _, queue := range mh.inputQueues {
		go func(q middleware.MessageMiddleware) {
			if err := mh.consumeFromQueue(q, mh.inputChannel); err != nil {
				log.Errorf("Failed to consume from queue: %v", err)
			}
		}(queue)
	}

	log.Debug("All input queues are now consuming.")

	// GLT! (Geodude Likes This)
	for mh.isRunning {

		select {
		case dataEnvelope := <-mh.inputChannel:

			if err := mh.dataHandler.HandleData(dataEnvelope); err != nil {
				log.Warnf("Failed to handle data batch: %v", err)
				return err
			}

		case finishEnvelope := <-mh.finisherChannel:
			clientID := finishEnvelope.GetClientId()
			log.Debugf("Received finish message for client: %s", clientID)
			mh.dataHandler.HandleFinishClient(clientID)

		}
	}
	return nil
}

// Shuts down all connections and stops all consumption
func (mh *messageHandler) Close() error {
	mh.isRunning = false
	close(mh.inputChannel)
	for _, queue := range mh.inputQueues {
		queue.StopConsuming()
		if e := queue.Close(); e != middleware.MessageMiddlewareSuccess {
			log.Errorf("Failed to close input queue: %d", int(e))
		}
	}
	if mh.finisherExchange != nil {
		mh.finisherExchange.StopConsuming()
		if e := mh.finisherExchange.Close(); e != middleware.MessageMiddlewareSuccess {
			log.Errorf("Failed to close finisher queue: %d", int(e))
		}
	}
	err := mh.dataHandler.Close()
	if err != nil {
		log.Errorf("Failed to close data handler: %v", err)
		return err
	}
	log.Debug("MessageHandler closed successfully.")
	return nil
}

/* --- PRIVATE UTIL METHODS --- */

// Starts consuming for input queues indefinitely and placeing received messages into a monitor channel
func (mh *messageHandler) consumeFromQueue(inputQueue middleware.MessageMiddleware, channelOutput chan *protocol.DataEnvelope) error {

	log.Debugf("Starting to consume from queue")

	done := make(chan bool, 1)

	e := inputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {

			// TODO: change when ACK actually occures,
			// not just when received but actually after processed and sent to next queue
			msg.Ack(false)

			dataBatch, err := utils.GetDataEnvelope(msg.Body)

			if err != nil {
				log.Errorf("Failed to unmarshal message: %v", err)
				done <- true
				return
			}
			channelOutput <- dataBatch

		}
		done <- true
	})
	<-done

	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("A error occurred while starting consumption: %d", int(e))
	}

	return nil
}

/* --- MESSAGE HANDLER SEND DATA FUNCTION --- */

// SendDataToMiddleware is a utility function to send data to a middleware queue
func SendDataToMiddleware(data proto.Message, taskType enum.TaskType, clientID string, outputQueue middleware.MessageMiddleware) error {

	envelope, err := utils.CreateSerializedEnvelope(data, int32(taskType), clientID)
	if err != nil {
		return fmt.Errorf("failed to envelope message: %v", err)
	}

	if e := outputQueue.Send(envelope); e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to send message to output queue: %d", int(e))
	}

	return nil
}

func SendDone(clientID string, taskType enum.TaskType, outputQueue middleware.MessageMiddleware) error {
	dataEnvelope := &protocol.DataEnvelope{
		ClientId: clientID,
		TaskType: int32(taskType),
		IsDone:   true,
	}

	data, err := proto.Marshal(dataEnvelope)

	if err != nil {
		return fmt.Errorf("failed to serialize done message: %v", err)
	}

	if e := outputQueue.Send(data); e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("failed to send message to output queue: %d", int(e))
	}

	return nil
}
