package session

import (
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/handler"
)

type ClientManager struct {
	clients map[int]*clientSession
	nextID  int
}

func NewClientManager() *ClientManager {
	return &ClientManager{
		clients: make(map[int]*clientSession),
	}
}

func (cm *ClientManager) AddClient(connection *network.ConnectionInterface, taskHandler handler.Handler) *clientSession {
	cm.nextID++
	session := NewClientSession(cm.nextID, connection, taskHandler)
	cm.clients[cm.nextID] = session
	return session
}

func (cm *ClientManager) RemoveClient(id int) {
	delete(cm.clients, id)
}

func (cm *ClientManager) Close() {
	for id, session := range cm.clients {
		session.Close()
		delete(cm.clients, id)
	}
}
