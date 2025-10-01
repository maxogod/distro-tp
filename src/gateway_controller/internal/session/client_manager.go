package session

import (
	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/handler"
)

type ClientManager struct {
	clients map[string]*clientSession
}

func NewClientManager() *ClientManager {
	return &ClientManager{
		clients: make(map[string]*clientSession),
	}
}

func (cm *ClientManager) AddClient(connection *network.ConnectionInterface, taskHandler handler.Handler) *clientSession {
	id := uuid.New().String()
	session := NewClientSession(id, connection, taskHandler)
	cm.clients[id] = session
	return session
}

func (cm *ClientManager) RemoveClient(id string) {
	delete(cm.clients, id)
}

func (cm *ClientManager) Close() {
	for id, session := range cm.clients {
		session.Close()
		delete(cm.clients, id)
	}
}
