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
	var taskType enum.TaskType

	for processData {
		request, err := cs.getRequest()
		if err != nil {
			return err
		}
		request.ClientId = cs.Id

		if request.GetIsRef() {
			cs.taskHandler.HandleReferenceData(request, cs.Id)
		} else if request.GetIsDone() {
			taskType = enum.TaskType(request.GetTaskType())
			processData = false
		} else {
			cs.taskHandler.HandleTask(taskType, request)
		}
	}

	log.Debugln("All data received from client, sending done signal to task handler")
	err := cs.taskHandler.SendDone(taskType, cs.Id)
	if err != nil {
		return err
	}

	log.Debugln("Starting to send report data to client")
	err = cs.processResponse()
	if err != nil {
		return err
	}

	// TODO: tell joiners to remove persisted data

	log.Debugf("All report data sent to client %s, closing session", cs.Id)
	cs.taskHandler.Reset()

	cs.running = false

	return nil
}

func (cs *clientSession) Close() {
	cs.clientConnection.Close()
}

// --- PRIVATE METHODS ---

func (cs *clientSession) processResponse() error {
	data := make(chan []byte)
	disconnect := make(chan bool)
	isDone := false
	for cs.clientConnection.IsConnected() && !isDone {
		go cs.taskHandler.GetReportData(data, disconnect)

		for batch := range data {
			dataBatch := &protocol.DataEnvelope{}
			err := proto.Unmarshal(batch, dataBatch)
			if err != nil || err == nil && dataBatch.GetClientId() != cs.Id {
				continue
			}

			cs.clientConnection.SendData(batch)

			if dataBatch.GetIsDone() {
				log.Debugln("Received done signal from task handler")
				isDone = true
			}
		}
	}
	disconnect <- true
	return nil
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
