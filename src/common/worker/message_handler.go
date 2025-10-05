package worker

import (
	"fmt"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/utils"
)

var log = logger.GetLogger()

const CLIENT_TIMEOUT = 10 * time.Second

// when having to handle / finish clients, there is a bool to indicate if
// the client is in finishing mode, and a timer to count down to when it should execute the
// FinishClient function
type Client struct {
	finishUp bool
	timer    *time.Timer
}

// This struct is required when creating a worker that consumes
// from input queues and produces to output queues
// passing data to a DataHandler function, and sending results to output queues
// It is required to have a DataHandler implementation to handle the data
type MessageHandler struct {
	// connections
	inputQueues   []middleware.MessageMiddleware
	finisherQueue middleware.MessageMiddleware

	// internals
	dataHandler    DataHandler
	monitorChannel chan *protocol.DataEnvelope
	clientManager  map[string]Client
	isRunning      bool //GLT
}

func NewMessageHandler(
	dataHandler DataHandler,
	inputQueues []middleware.MessageMiddleware,
	finisherQueue middleware.MessageMiddleware,
) *MessageHandler {

	mh := &MessageHandler{
		dataHandler:    dataHandler,
		inputQueues:    inputQueues,
		monitorChannel: make(chan *protocol.DataEnvelope),
		isRunning:      true,
	}

	// if this requires a finisher queue,
	// then when a finish message is recieved, a timer
	// begins to wrap up any consumption of a specific clients messages
	if finisherQueue != nil {
		mh.finisherQueue = finisherQueue
		mh.clientManager = make(map[string]Client)
		if err := mh.setupFinishListener(); err != nil {
			log.Errorf("Failed to set up finish listener: %v", err)
		}
	}

	return mh
}

// Shuts down all connections and stops all consumption
func (mh *MessageHandler) Close() error {
	mh.isRunning = false
	close(mh.monitorChannel)

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

	return nil
}

// Starts consuming for input queues indefinitely and placeing received messages into a monitor channel
func (mh *MessageHandler) consumeFromQueue(inputQueue middleware.MessageMiddleware, channelOutput chan *protocol.DataEnvelope) error {

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

// Starts consuming indefinetly from monitor channel and handling messages with the provided dataHandler function
func (mh *MessageHandler) Start() error {

	log.Debug("Starting MessageHandler...")
	for _, queue := range mh.inputQueues {
		go func(q middleware.MessageMiddleware) {
			if err := mh.consumeFromQueue(q, mh.monitorChannel); err != nil {
				log.Errorf("Failed to consume from queue: %v", err)
			}
		}(queue)
	}

	log.Debugf("All input queues are now consuming.")

	// GLT! (Geodude Likes This)
	for mh.isRunning {

		dataEnvelope := <-mh.monitorChannel

		clientID := dataEnvelope.GetClientId()

		mh.checkClient(clientID)

		if err := mh.dataHandler.HandleData(dataEnvelope); err != nil {
			log.Errorf("Failed to handle data batch: %v", err)
		}
	}
	return nil
}

func (mh *MessageHandler) setupFinishListener() error {

	finisherChannel := make(chan *protocol.DataEnvelope)

	go func() {
		if err := mh.consumeFromQueue(mh.finisherQueue, finisherChannel); err != nil {
			log.Errorf("Failed to consume from finisher queue: %v", err)
		}
	}()

	go func() {
		for mh.isRunning {
			dataEnvelope := <-finisherChannel
			clientID := dataEnvelope.GetClientId()
			if clientID == "" {
				log.Warn("Received finish message with empty client ID")
				continue
			}

			log.Debugf("Received finish message for client: %s", clientID)

			if _, exists := mh.clientManager[clientID]; exists {
				mh.clientManager[clientID] = Client{
					finishUp: true,
					timer: time.AfterFunc(CLIENT_TIMEOUT, func() {
						mh.FinishClient(clientID)
					}),
				}
			} else {
				log.Warnf("Received finish message for unknown client: %s", clientID)
			}
		}
	}()

	return nil
}

func (mh *MessageHandler) checkClient(clientID string) {
	if clientID == "" || mh.finisherQueue == nil || mh.clientManager == nil {
		return
	}

	log.Debugf("Checking client: %s", clientID)

	if _, exists := mh.clientManager[clientID]; !exists {
		mh.clientManager[clientID] = Client{
			finishUp: false,
			timer:    nil,
		}
		return
	}

	client := mh.clientManager[clientID]

	if client.finishUp {
		// If the client is in finishing mode, reset the timer
		if client.timer != nil {
			log.Debugf("Resetting timer for client: %s", clientID)
			client.timer.Stop()
		}
		client.timer = time.AfterFunc(CLIENT_TIMEOUT, func() {
			log.Debugf("Timer expired for client: %s", clientID)
			mh.FinishClient(clientID)
		})
		mh.clientManager[clientID] = client
	}

}

func (mh *MessageHandler) FinishClient(clientID string) error {

	log.Debugf("Finishing client: %s", clientID)

	if err := mh.dataHandler.HandleFinishClient(clientID); err != nil {
		log.Errorf("Failed to finish client %s: %v", clientID, err)
		return err
	}
	delete(mh.clientManager, clientID)
	return nil
}
