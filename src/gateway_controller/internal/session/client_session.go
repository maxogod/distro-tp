package session

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/handler"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type clientSession struct {
	Id               string
	clientConnection *network.ConnectionInterface
	taskHandler      handler.Handler
	processData      bool
}

func NewClientSession(id string, conn *network.ConnectionInterface, taskHandler handler.Handler) *clientSession {
	return &clientSession{
		Id:               id,
		clientConnection: conn,
		taskHandler:      taskHandler,
		processData:      true,
	}
}

func (cs *clientSession) ProcessRequest() error {
	log.Debugln("Starting to process client requests")
	// i know that the variable is not necesary but geodude likes this handling
	var taskType enum.TaskType
	for cs.processData {
		request, err := cs.getRequest()
		if err != nil {
			return err
		}
		request.ClientId = cs.Id

		taskType = enum.TaskType(request.GetTaskType())
		isRefData := request.GetIsReferenceData()

		if isRefData {
			// forward reference data including done signal to task handler
			cs.taskHandler.HandleReferenceData(request, cs.Id)
			continue
		} else if request.GetDone() {
			cs.processData = false
			break
		}
		cs.taskHandler.HandleTask(taskType, request)
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

	log.Debugln("All report data sent to client, closing session")

	cs.taskHandler.Reset()

	return nil
}

func (cs *clientSession) processResponse() error {
	data := make(chan []byte)
	disconnect := make(chan bool)
	isDone := false
	for cs.clientConnection.IsConnected() && !isDone {
		go cs.taskHandler.GetReportData(data, disconnect)

		for batch := range data {
			dataBatch := &data_batch.DataBatch{}
			err := proto.Unmarshal(batch, dataBatch)
			if err != nil {
				isDone = true
				break
			}
			if dataBatch.GetClientId() != cs.Id {
				continue
			}

			err = cs.clientConnection.SendData(batch)
			if err != nil {
				isDone = true
				break
			}

			// If the process data is finished, break the loop
			if dataBatch.GetDone() {
				log.Debugln("Received done signal from task handler")
				isDone = true
				break
			}
		}
	}
	disconnect <- true
	return nil
}

func (cs *clientSession) getRequest() (*data_batch.DataBatch, error) {
	requestBytes, err := cs.clientConnection.ReceiveData()
	if err != nil {
		log.Errorf("Error receiving request type: %v", err)
		return nil, err
	}
	request := &data_batch.DataBatch{}
	err = proto.Unmarshal(requestBytes, request)
	if err != nil {
		log.Errorf("Error receiving request type: %v", err)
		return nil, err
	}
	return request, nil
}

// TODO: Remove deprecated
func (cs *clientSession) sendReportData(reportData []byte) error {
	cs.clientConnection.SendData(reportData)
	return nil
}

// TODO: Remove deprecated
func (cs *clientSession) HandleReferenceData(response *data_batch.DataBatch) error {
	return nil
}

func (cs *clientSession) Close() {
	cs.clientConnection.Close()
}
