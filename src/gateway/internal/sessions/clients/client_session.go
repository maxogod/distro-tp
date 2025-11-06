package clients

import (
	"sync/atomic"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/internal/handler"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type clientSession struct {
	Id               string
	clientConnection network.ConnectionInterface
	messageHandler   handler.MessageHandler
	running          atomic.Bool
}

func NewClientSession(id string, conn network.ConnectionInterface, messageHandler handler.MessageHandler) ClientSession {
	s := &clientSession{
		Id:               id,
		clientConnection: conn,
		messageHandler:   messageHandler,
	}
	s.running.Store(true)
	return s
}

func (cs *clientSession) IsFinished() bool {
	return !cs.running.Load()
}

func (cs *clientSession) ProcessRequest() error {
	log.Debugf("[%s] Starting to process client request", cs.Id)

	// Initialize session with controller
	err := cs.messageHandler.AwaitControllerInit()
	if err != nil {
		log.Errorf("[%s] Error awaiting controller init for client: %v", cs.Id, err)
		return err
	}

	err = cs.messageHandler.NotifyClientMessagesCount()
	if err != nil {
		log.Errorf("[%s] Error notifying controller about client messages count: %v", cs.Id, err)
		return err
	}

	// Start processing
	processData := true
	for processData {
		request, err := cs.getRequest()
		if err != nil {
			log.Errorf("[%s] Error getting request from client: %v", cs.Id, err)
			return err
		}
		request.ClientId = cs.Id

		if request.GetIsRef() {
			cs.messageHandler.ForwardReferenceData(request)
		} else if request.GetIsDone() {
			processData = false
		} else {
			cs.messageHandler.ForwardData(request)
		}
	}

	log.Debugf("[%s] Starting to send report data to client", cs.Id)
	cs.processResponse()

	cs.Close()
	log.Debugf("[%s] All report data sent to client, and session closed", cs.Id)

	return nil
}

func (cs *clientSession) Close() {
	if !cs.IsFinished() {
		cs.clientConnection.Close()
		cs.messageHandler.Close()
		cs.running.Store(false)
		log.Debugf("[%s] Closed client session", cs.Id)
	}
}

/* --- PRIVATE METHODS --- */

func (cs *clientSession) processResponse() {
	data := make(chan *protocol.DataEnvelope)
	go cs.messageHandler.GetReportData(data)

	// Read and send until channel is closed
	for batch := range data {
		dataBytes, err := proto.Marshal(batch)
		if err != nil {
			log.Errorf("[%s] Error marshaling data to send to client: %v", cs.Id, err)
			continue
		}
		cs.clientConnection.SendData(dataBytes)
	}
}

func (cs *clientSession) getRequest() (*protocol.DataEnvelope, error) {
	requestBytes, err := cs.clientConnection.ReceiveData()
	if err != nil {
		log.Errorf("[%s] Error receiving data: %v", cs.Id, err)
		return nil, err
	}

	request := &protocol.DataEnvelope{}
	err = proto.Unmarshal(requestBytes, request)
	if err != nil {
		log.Errorf("[%s] Error receiving data: %v", cs.Id, err)
		return nil, err
	}

	return request, nil
}
