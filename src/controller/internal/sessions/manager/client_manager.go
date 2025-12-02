package manager

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
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

func (cm *clientManager) AddClient(id string, taskType enum.TaskType) clients.ClientSession {
	if _, exists := cm.clients[id]; exists {
		logger.Logger.Debugf("action: add_client | client_id: %s already exists for tasktype %s", id, string(taskType))
		clientSession := cm.clients[id]
		clientSession.SendControllerReady()
		return nil
	}

	controlHandler := handler.NewControlHandler(cm.config.MiddlewareAddress, id, taskType, cm.config.CompletionAfterDoneTimeout)
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
