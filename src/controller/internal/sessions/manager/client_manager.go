package manager

import (
	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/controller/config"
	"github.com/maxogod/distro-tp/src/controller/internal/handler"
	"github.com/maxogod/distro-tp/src/controller/internal/sessions/clients"
)

type clientManager struct {
	clients map[string]clients.ClientSession
	config  *config.Config
}

func NewClientManager(conf *config.Config) ClientManager {
	return &clientManager{
		clients: make(map[string]clients.ClientSession),
		config:  conf,
	}
}

func (cm *clientManager) AddClient(id string) clients.ClientSession {
	controlHandler := handler.NewControlHandler(cm.config.MiddlewareAddress, id)
	session := clients.NewClientSession(id, controlHandler)
	cm.clients[id] = session
	return session
}

func (cm *clientManager) RemoveClient(id string) {
	delete(cm.clients, id)
}

func (cm *clientManager) ReapStaleClients() {
	for id, session := range cm.clients {
		if session.IsFinished() {
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
