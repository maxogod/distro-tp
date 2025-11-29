package manager

import (
	"strconv"
	"sync"

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
	idBytes, err := connection.ReceiveData()
	if err != nil {
		logger.Logger.Errorf("Error receiving ID for client: %v", err)
		return nil
	}

	id := string(idBytes)
	logger.Logger.Infof("Client connected with ID: %s", id)

	// Asumo que soy el líder
	// TODO: Usar el LeaderElection y mandar un Protobuf en vez del ID directamente
	if err = connection.SendData([]byte(strconv.Itoa(cm.config.LeaderElection.ID))); err != nil {
		logger.Logger.Errorf("Error sending leader ID to client %s: %v", id, err)
		return nil
	}

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
