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

const CLIENT_TIMEOUT = 10 * time.Second

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
	inputQueues   []middleware.MessageMiddleware
	finisherQueue middleware.MessageMiddleware

	// internals
	dataHandler         DataHandler
	inputChannel        chan *protocol.DataEnvelope
	finisherChannel     chan *protocol.DataEnvelope
	handleFinishChannel chan string
	clientManager       map[string]client
	isRunning           bool //GLT
}

func NewMessageHandler(
	dataHandler DataHandler,
	inputQueues []middleware.MessageMiddleware,
	finisherQueue middleware.MessageMiddleware,
) MessageHandler {

	mh := &messageHandler{
		dataHandler:         dataHandler,
		inputQueues:         inputQueues,
		inputChannel:        make(chan *protocol.DataEnvelope),
		finisherChannel:     make(chan *protocol.DataEnvelope),
		handleFinishChannel: make(chan string),
		isRunning:           true,
	}

	// if this requires a finisher queue,
	// then when a finish message is recieved, a timer
	// begins to wrap up any consumption of a specific clients messages
	if finisherQueue != nil {
		mh.finisherQueue = finisherQueue
		mh.clientManager = make(map[string]client)
		log.Debug("Finisher queue consuming.")
		go func() {
			if err := mh.consumeFromQueue(mh.finisherQueue, mh.finisherChannel); err != nil {
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

			clientID := dataEnvelope.GetClientId()

			mh.checkClient(clientID)

			if err := mh.dataHandler.HandleData(dataEnvelope); err != nil {
				log.Warnf("Failed to handle data batch: %v", err)
				return err
			}

		case finishEnvelope := <-mh.finisherChannel:
			clientID := finishEnvelope.GetClientId()
			log.Debugf("Received finish message for client: %s", clientID)

			if _, exists := mh.clientManager[clientID]; exists {
				mh.clientManager[clientID] = client{
					finishUp: true,
					timer: time.AfterFunc(CLIENT_TIMEOUT, func() {
						mh.finishClient(clientID)
					}),
				}
			} else {
				log.Warnf("Received finish message for unknown client: %s", clientID)
			}
		case finishedClientID := <-mh.handleFinishChannel:
			log.Debugf("Reaping client: %s", finishedClientID)
			err := mh.dataHandler.HandleFinishClient(finishedClientID)
			if err != nil {
				log.Warnf("Failed to finish client %s: %v", finishedClientID, err)
			}
			delete(mh.clientManager, finishedClientID)
		}
	}
	return nil
}

// To avoid race conditions, this function should only be called from within the messageHandler's main loop
func (mh *messageHandler) finishClient(clientID string) error {
	mh.handleFinishChannel <- clientID
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

	if mh.finisherQueue != nil {
		mh.finisherQueue.StopConsuming()
		if e := mh.finisherQueue.Close(); e != middleware.MessageMiddlewareSuccess {
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

func (mh *messageHandler) checkClient(clientID string) {
	if clientID == "" || mh.finisherQueue == nil || mh.clientManager == nil {
		return
	}

	if _, exists := mh.clientManager[clientID]; !exists {
		log.Debugf("New client detected: %s", clientID)
		mh.clientManager[clientID] = client{
			finishUp: false,
			timer:    nil,
		}
		return
	}

	client := mh.clientManager[clientID]

	if client.finishUp {
		// If the client is in finishing mode, reset the timer
		if client.timer != nil {
			client.timer.Stop()
		}
		client.timer = time.AfterFunc(CLIENT_TIMEOUT, func() {
			log.Debugf("Timer expired for client: %s", clientID)
			mh.finishClient(clientID)
		})
		mh.clientManager[clientID] = client
	}

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

func SendDone(clientID string, outputQueue middleware.MessageMiddleware) error {

	dataEnvelope := &protocol.DataEnvelope{
		ClientId: clientID,
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
