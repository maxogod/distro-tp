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
	Id               int
	clientConnection *network.ConnectionInterface
	taskHandler      *handler.TaskHandler
	processData      bool
}

func NewClientSession(id int, conn *network.ConnectionInterface, taskHandler *handler.TaskHandler) *clientSession {
	return &clientSession{
		Id:               id,
		clientConnection: conn,
		taskHandler:      taskHandler,
		processData:      true,
	}
}

func (cs *clientSession) ProcessRequest() error {

	// i know that the variable is not necesary but geodude likes this handling
	for cs.processData {

		request, err := cs.getRequest()
		if err != nil {
			return err
		}

		taskType := enum.TaskType(request.GetTaskType())
		isRefData := request.GetIsReferenceData()

		if isRefData {
			return cs.taskHandler.HandleReferenceData(request)
		} else if request.GetDone() {
			cs.processData = false
			break
		}
		cs.taskHandler.HandleTask(taskType, request)
	}

	err := cs.taskHandler.SendDone()
	if err != nil {
		return err
	}
	reportData, err := cs.taskHandler.GetReportData()
	if err != nil {
		return err
	}

	err = cs.sendReportData(reportData)
	if err != nil {
		return err
	}

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

func (cs *clientSession) sendReportData(reportData []byte) error {

	err := cs.clientConnection.SendData(reportData)
	if err != nil {
		return err
	}
	return nil
}

func (cs *clientSession) HandleReferenceData(response *data_batch.DataBatch) error {

	return nil

}

func (cs *clientSession) Close() error {
	return cs.clientConnection.Close()
}
