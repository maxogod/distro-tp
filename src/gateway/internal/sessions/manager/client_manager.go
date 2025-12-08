package manager

import (
	"sync"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/config"
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
	session, err := clients.NewClientSession(connection, cm.config)
	if err != nil {
		logger.Logger.Errorf("Error creating client session: %v", err)
		return nil
	}

	cm.clients.Store(session.GetClientId(), session)
	logger.Logger.Infof("Client connected with ID: %s", session.GetClientId())
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
			cm.RemoveClient(id)
		}

		return true
	})
}

func (cm *clientManager) Close() {
	cm.clients.Range(func(key, value any) bool {
		id := key.(string)
		session := value.(clients.ClientSession)

		session.Close()
		cm.RemoveClient(id)

		return true
	})
}
