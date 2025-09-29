package session

import (
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/handler"
)

type clientSession struct {
	Id               int
	clientConnection *network.ConnectionInterface
	taskHandler      *handler.TaskHandler
}

func NewClientSession(id int, conn *network.ConnectionInterface, taskHandler *handler.TaskHandler) *clientSession {
	return &clientSession{
		Id:               id,
		clientConnection: conn,
		taskHandler:      taskHandler,
	}
}

func (cs *clientSession) HandleRequest() error {

	return nil

}

func (cs *clientSession) RecieveRequest() {
	// TODO: see if the request is for transaction data or reference data
}

func (cs *clientSession) HandleTransactionData() {
	// after receiving the request, handle it with the task handler

}

func (cs *clientSession) HandleReferenceData() {
	// after receiving the request, handle it with the task handler

}

func (cs *clientSession) SendClientReport(response any) error {
	// TODO: figure out how to send a report to the client
	return nil
}

func (cs *clientSession) Close() error {
	return cs.clientConnection.Close()
}
