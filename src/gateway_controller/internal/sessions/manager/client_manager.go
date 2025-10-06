package manager

import (
	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/handler"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/sessions/clients"
)

type clientManager struct {
	clients map[string]clients.ClientSession
}

func NewClientManager() ClientManager {
	return &clientManager{
		clients: make(map[string]clients.ClientSession),
	}
}

func (cm *clientManager) AddClient(connection network.ConnectionInterface, taskHandler handler.Handler) clients.ClientSession {
	id := uuid.New().String()
	session := clients.NewClientSession(id, connection, taskHandler)
	cm.clients[id] = session
	return session
}

func (cm *clientManager) RemoveClient(id string) {
	delete(cm.clients, id)
}

func (cm *clientManager) ReapStaleClients() {
	for id, session := range cm.clients {
		if session.IsFinished() {
			session.Close()
			delete(cm.clients, id)
		}
	}
}

func (cm *clientManager) Close() {
	for id, session := range cm.clients {
		session.Close()
		delete(cm.clients, id)
	}
}
