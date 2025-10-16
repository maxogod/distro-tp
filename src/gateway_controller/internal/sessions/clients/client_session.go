package clients

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/handler"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type clientSession struct {
	Id               string
	clientConnection network.ConnectionInterface
	messageHandler   handler.MessageHandler
	running          bool
}

func NewClientSession(id string, conn network.ConnectionInterface, messageHandler handler.MessageHandler) ClientSession {
	return &clientSession{
		Id:               id,
		clientConnection: conn,
		messageHandler:   messageHandler,
		running:          true,
	}
}

func (cs *clientSession) IsFinished() bool {
	return !cs.running
}

func (cs *clientSession) ProcessRequest() error {
	log.Debugf("Starting to process client request for: %s", cs.Id)

	processData := true
	for processData {
		request, err := cs.getRequest()
		if err != nil {
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

	log.Debugln("All data received from client, sending done signal to task handler")
	err := cs.messageHandler.SendDone(enum.AggregatorWorker)
	if err != nil {
		return err
	}

	log.Debugln("Starting to send report data to client")
	cs.processResponse()

	err = cs.messageHandler.SendDone(enum.JoinerWorker)
	if err != nil {
		return err
	}

	log.Debugf("All report data sent to client %s, closing session", cs.Id)
	cs.running = false

	return nil
}

func (cs *clientSession) Close() {
	cs.clientConnection.Close()
}

// --- PRIVATE METHODS ---

func (cs *clientSession) processResponse() {
	data := make(chan *protocol.DataEnvelope)
	go cs.messageHandler.GetReportData(data)

	// Read and send until channel is closed
	for batch := range data {
		dataBytes, err := proto.Marshal(batch)
		if err != nil {
			log.Errorf("Error marshaling data to send to client: %v", err)
			continue
		}
		cs.clientConnection.SendData(dataBytes)
	}
}

func (cs *clientSession) getRequest() (*protocol.DataEnvelope, error) {
	requestBytes, err := cs.clientConnection.ReceiveData()
	if err != nil {
		log.Errorf("Error receiving data: %v", err)
		return nil, err
	}

	request := &protocol.DataEnvelope{}
	err = proto.Unmarshal(requestBytes, request)
	if err != nil {
		log.Errorf("Error receiving data: %v", err)
		return nil, err
	}

	return request, nil
}
