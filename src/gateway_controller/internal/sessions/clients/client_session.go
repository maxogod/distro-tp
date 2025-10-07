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
	taskHandler      handler.Handler
	running          bool
}

func NewClientSession(id string, conn network.ConnectionInterface, taskHandler handler.Handler) ClientSession {
	return &clientSession{
		Id:               id,
		clientConnection: conn,
		taskHandler:      taskHandler,
		running:          true,
	}
}

func (cs *clientSession) IsFinished() bool {
	return !cs.running
}

func (cs *clientSession) ProcessRequest() error {
	log.Debugln("Starting to process client requests")

	processData := true
	for processData {
		request, err := cs.getRequest()
		if err != nil {
			return err
		}
		request.ClientId = cs.Id

		if request.GetIsRef() {
			cs.taskHandler.ForwardReferenceData(request, cs.Id)
		} else if request.GetIsDone() {
			processData = false
		} else {
			cs.taskHandler.ForwardData(request, cs.Id)
		}
	}

	log.Debugln("All data received from client, sending done signal to task handler")
	err := cs.taskHandler.SendDone(enum.AggregatorWorker, cs.Id)
	if err != nil {
		return err
	}

	log.Debugln("Starting to send report data to client")
	cs.processResponse()

	err = cs.taskHandler.SendDone(enum.JoinerWorker, cs.Id)
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
	go cs.taskHandler.GetReportData(data, cs.Id)

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
