package clients

import (
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/config"
	"github.com/maxogod/distro-tp/src/gateway/internal/handler"
	"google.golang.org/protobuf/proto"
)

type clientSession struct {
	clientId         string
	taskType         enum.TaskType
	clientConnection network.ConnectionInterface
	messageHandler   handler.MessageHandler
	running          atomic.Bool

	seqNumsReceived map[int32]bool
}

func NewClientSession(conn network.ConnectionInterface, config *config.Config) ClientSession {
	cs := &clientSession{
		clientConnection: conn,
		seqNumsReceived:  make(map[int32]bool),
	}

	controlMsg, err := cs.getControlRequest()
	if err != nil {
		if cs.IsFinished() {
			logger.Logger.Infof("[%s] Client session is finished, stopping control request processing", cs.clientId)
			return nil
		}
		logger.Logger.Errorf("[%s] Error getting task request: %v", cs.clientId, err)
		return nil
	}

	oldClientId := controlMsg.GetClientId()
	cs.taskType = enum.TaskType(controlMsg.GetTaskType())

	if oldClientId == uuid.Nil.String() || cs.taskType == enum.T1 {
		logger.Logger.Debugf("New client connected, ID: %s", cs.clientId)
		cs.clientId = uuid.New().String()
	} else {
		logger.Logger.Debugf("Reconnected client with ID: %s", cs.clientId)
		cs.clientId = oldClientId
	}

	cs.messageHandler = handler.NewMessageHandler(config.MiddlewareAddress, cs.clientId, config.ReceivingTimeout)
	if oldClientId != uuid.Nil.String() && cs.taskType == enum.T1 {
		logger.Logger.Debugf("Aborting client with ID: %s for task %s", oldClientId, string(cs.taskType))
		notifErr := cs.messageHandler.NotifyCompletion(oldClientId, true)
		if notifErr != nil {
			logger.Logger.Errorf("[%s] Error notifying controller about reconnection of clientId %s: %v", cs.clientId, oldClientId, notifErr)
			return nil
		}
	}

	cs.running.Store(true)

	return cs
}

func (cs *clientSession) GetClientId() string {
	return cs.clientId
}

func (cs *clientSession) IsFinished() bool {
	return !cs.running.Load()
}

func (cs *clientSession) ProcessRequest() error {
	logger.Logger.Debugf("[%s] Starting to process client request", cs.clientId)

	// Initialize session with controller
	err := cs.messageHandler.SendControllerInit(cs.taskType)
	if err != nil {
		return err
	}

	err = cs.messageHandler.AwaitControllerInit()
	if err != nil {
		logger.Logger.Errorf("[%s] Error awaiting controller init for client: %v", cs.clientId, err)
		return err
	}

	err = cs.sendClientRequestAck(cs.taskType)
	if err != nil {
		return err
	}

	// Start processing
	processData := true
	for processData {
		request, err := cs.getRequest()
		if err != nil {
			if cs.IsFinished() {
				logger.Logger.Infof("[%s] Client session is finished, stopping data request processing", cs.clientId)
				return nil
			}
			logger.Logger.Errorf("[%s] Error getting request from client: %v", cs.clientId, err)
			return err
		}
		request.ClientId = cs.clientId

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
		logger.Logger.Errorf("[%s] Error notifying controller about client messages count: %v", cs.clientId, err)
		return err
	}

	logger.Logger.Debugf("[%s] Starting to send report data to client", cs.clientId)
	cs.processResponse()

	err = cs.messageHandler.NotifyCompletion(cs.clientId, false)
	if err != nil {
		logger.Logger.Errorf("[%s] Error notifying controller about client completion: %v", cs.clientId, err)
		return err
	}

	cs.Close()
	logger.Logger.Debugf("[%s] All report data sent to client, and session closed", cs.clientId)

	return nil
}

func (cs *clientSession) sendClientRequestAck(taskType enum.TaskType) error {
	requestAck := &protocol.ControlMessage{
		ClientId: cs.clientId,
		TaskType: int32(taskType),
		IsAck:    true,
	}

	ackBytes, err := proto.Marshal(requestAck)
	if err != nil {
		logger.Logger.Errorf("[%s] Error marshaling ack response: %v", cs.clientId, err)
		return err
	}

	if err = cs.clientConnection.SendData(ackBytes); err != nil {
		logger.Logger.Errorf("[%s] Error sending ack response: %v", cs.clientId, err)
		return err
	}
	return nil
}

func (cs *clientSession) Close() {
	if !cs.IsFinished() {
		cs.running.Store(false)
		cs.clientConnection.Close()
		cs.messageHandler.Close()
		logger.Logger.Debugf("[%s] Closed client session", cs.clientId)
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
			logger.Logger.Debugf("[%s] Duplicate sequence number %d in report data. Ignoring message.", cs.clientId, seq)
			continue
		}
		cs.seqNumsReceived[seq] = true

		dataBytes, err := proto.Marshal(batch)
		if err != nil {
			logger.Logger.Errorf("[%s] Error marshaling data to send to client: %v", cs.clientId, err)
			continue
		}
		cs.clientConnection.SendData(dataBytes)
	}
}

func (cs *clientSession) getRequest() (*protocol.DataEnvelope, error) {
	requestBytes, err := cs.clientConnection.ReceiveData()
	if err != nil {
		return nil, err
	}

	request := &protocol.DataEnvelope{}
	err = proto.Unmarshal(requestBytes, request)
	if err != nil {
		logger.Logger.Errorf("[%s] Error receiving data: %v", cs.clientId, err)
		return nil, err
	}

	return request, nil
}

func (cs *clientSession) getControlRequest() (*protocol.ControlMessage, error) {
	requestBytes, err := cs.clientConnection.ReceiveData()
	if err != nil {
		return nil, err
	}

	request := &protocol.ControlMessage{}
	err = proto.Unmarshal(requestBytes, request)
	if err != nil {
		logger.Logger.Errorf("[%s] Error receiving data: %v", cs.clientId, err)
		return nil, err
	}

	return request, nil
}
