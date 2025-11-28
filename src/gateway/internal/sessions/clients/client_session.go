package clients

import (
	"sync/atomic"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/internal/handler"
	"google.golang.org/protobuf/proto"
)

type clientSession struct {
	Id               string
	clientConnection network.ConnectionInterface
	messageHandler   handler.MessageHandler
	running          atomic.Bool

	seqNumsReceived map[int32]bool
}

func NewClientSession(id string, conn network.ConnectionInterface, messageHandler handler.MessageHandler) ClientSession {
	s := &clientSession{
		Id:               id,
		clientConnection: conn,
		messageHandler:   messageHandler,

		seqNumsReceived: make(map[int32]bool),
	}
	s.running.Store(true)
	return s
}

func (cs *clientSession) IsFinished() bool {
	return !cs.running.Load()
}

func (cs *clientSession) ProcessRequest() error {
	logger.Logger.Debugf("[%s] Starting to process client request", cs.Id)

	controlMsg, err := cs.getControlRequest()
	if err != nil {
		logger.Logger.Errorf("[%s] Error getting task request: %v", cs.Id, err)
		return err
	}
	taskType := enum.TaskType(controlMsg.GetTaskType())

	// Initialize session with controller
	err = cs.messageHandler.AwaitControllerInit(taskType)
	if err != nil {
		logger.Logger.Errorf("[%s] Error awaiting controller init for client: %v", cs.Id, err)
		return err
	}

	// Start processing
	processData := true
	for processData {
		request, err := cs.getRequest()
		if err != nil {
			logger.Logger.Errorf("[%s] Error getting request from client: %v", cs.Id, err)
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

	err = cs.messageHandler.NotifyClientMessagesCount()
	if err != nil {
		logger.Logger.Errorf("[%s] Error notifying controller about client messages count: %v", cs.Id, err)
		return err
	}

	logger.Logger.Debugf("[%s] Starting to send report data to client", cs.Id)
	cs.processResponse()

	err = cs.messageHandler.NotifyCompletion()
	if err != nil {
		logger.Logger.Errorf("[%s] Error notifying controller about client completion: %v", cs.Id, err)
		return err
	}

	cs.Close()
	logger.Logger.Debugf("[%s] All report data sent to client, and session closed", cs.Id)

	return nil
}

func (cs *clientSession) Close() {
	if !cs.IsFinished() {
		cs.clientConnection.Close()
		cs.messageHandler.Close()
		cs.running.Store(false)
		logger.Logger.Debugf("[%s] Closed client session", cs.Id)
	}
}

/* --- PRIVATE METHODS --- */

func (cs *clientSession) processResponse() {
	data := make(chan *protocol.DataEnvelope)
	go cs.messageHandler.GetReportData(data)

	// Read and send until channel is closed
	for batch := range data {
		seq := batch.GetSequenceNumber()
		if _, exists := cs.seqNumsReceived[seq]; exists && !batch.GetIsDone() {
			logger.Logger.Debugf("[%s] Duplicate sequence number %d in report data. Ignoring message.", cs.Id, seq)
			continue
		}
		cs.seqNumsReceived[seq] = true

		dataBytes, err := proto.Marshal(batch)
		if err != nil {
			logger.Logger.Errorf("[%s] Error marshaling data to send to client: %v", cs.Id, err)
			continue
		}
		cs.clientConnection.SendData(dataBytes)
	}
}

func (cs *clientSession) getRequest() (*protocol.DataEnvelope, error) {
	requestBytes, err := cs.clientConnection.ReceiveData()
	if err != nil {
		logger.Logger.Errorf("[%s] Error receiving data: %v", cs.Id, err)
		return nil, err
	}

	request := &protocol.DataEnvelope{}
	err = proto.Unmarshal(requestBytes, request)
	if err != nil {
		logger.Logger.Errorf("[%s] Error receiving data: %v", cs.Id, err)
		return nil, err
	}

	return request, nil
}

func (cs *clientSession) getControlRequest() (*protocol.ControlMessage, error) {
	requestBytes, err := cs.clientConnection.ReceiveData()
	if err != nil {
		logger.Logger.Errorf("[%s] Error receiving data: %v", cs.Id, err)
		return nil, err
	}

	request := &protocol.ControlMessage{}
	err = proto.Unmarshal(requestBytes, request)
	if err != nil {
		logger.Logger.Errorf("[%s] Error receiving data: %v", cs.Id, err)
		return nil, err
	}

	return request, nil
}
