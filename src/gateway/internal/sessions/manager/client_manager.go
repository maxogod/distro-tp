package manager

import (
	"sync"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/config"
	"github.com/maxogod/distro-tp/src/gateway/internal/handler"
	"github.com/maxogod/distro-tp/src/gateway/internal/sessions/clients"
)

type clientManager struct {
	clients sync.Map
	config  *config.Config
}

func NewClientManager(conf *config.Config) ClientManager {
	return &clientManager{
		clients: sync.Map{},
		config:  conf,
	}
}

func (cm *clientManager) AddClient(connection network.ConnectionInterface) clients.ClientSession {
	id := uuid.New().String()
	logger.Logger.Infof("Client connected with ID: %s", id)
	messageHandler := handler.NewMessageHandler(cm.config.MiddlewareAddress, id, cm.config.ReceivingTimeout)
	session := clients.NewClientSession(id, connection, messageHandler)
	cm.clients.Store(id, session)
	return session
}

func (cm *clientManager) RemoveClient(id string) {
	cm.clients.Delete(id)
}

func (cm *clientManager) ReapStaleClients() {
	cm.clients.Range(func(key, value any) bool {
		id := key.(string)
		session := value.(clients.ClientSession)

		if session.IsFinished() {
			cm.clients.Delete(id)
		}

		return true
	})
}

func (cm *clientManager) Close() {
	cm.clients.Range(func(key, value any) bool {
		id := key.(string)
		session := value.(clients.ClientSession)

		session.Close()
		cm.clients.Delete(id)

		return true
	})
}
