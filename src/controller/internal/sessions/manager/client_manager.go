package manager

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/controller/config"
	"github.com/maxogod/distro-tp/src/controller/internal/handler"
	"github.com/maxogod/distro-tp/src/controller/internal/sessions/clients"
	"github.com/maxogod/distro-tp/src/controller/internal/storage"
)

type clientManager struct {
	clients map[string]clients.ClientSession
	config  *config.Config
	storage storage.CounterStorage
}

func NewClientManager(conf *config.Config, storage storage.CounterStorage) ClientManager {
	return &clientManager{
		clients: make(map[string]clients.ClientSession),
		config:  conf,
		storage: storage,
	}
}

func (cm *clientManager) AddClient(id string, taskType enum.TaskType, storedCounters []*protocol.MessageCounter) (clients.ClientSession, bool) {
	if _, exists := cm.clients[id]; exists {
		logger.Logger.Debugf("action: add_client | client_id: %s already exists for tasktype %s", id, string(taskType))
		return cm.clients[id], true
	}

	controlHandler := handler.NewControlHandler(
		cm.config.MiddlewareAddress,
		id,
		taskType,
		cm.storage,
		storedCounters,
		cm.config.MaxUnackedCounters,
	)
	session := clients.NewClientSession(id, controlHandler)
	cm.clients[id] = session
	return session, false
}

func (cm *clientManager) RemoveClient(id string) {
	session, ok := cm.clients[id]
	if !ok {
		return
	}

	session.Close()
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
