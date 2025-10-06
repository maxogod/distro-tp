package server

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/gateway_controller/business"
	"github.com/maxogod/distro-tp/src/gateway_controller/config"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/handler"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/network"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/sessions/manager"
)

var log = logger.GetLogger()

type Server struct {
	config            *config.Config
	running           bool
	workerService     *business.GatewayControllerService
	taskHandler       handler.Handler
	connectionManager network.ConnectionManager
	clientManager     manager.ClientManager
}

func NewServer(conf *config.Config) *Server {
	return &Server{
		config:            conf,
		running:           true,
		taskHandler:       handler.NewTaskHandler(business.NewControllerService(), conf.GatewayAddress),
		connectionManager: network.NewConnectionManager(conf.Port),
		clientManager:     manager.NewClientManager(),
	}
}

func (s *Server) Run() error {
	s.setupGracefulShutdown()
	defer s.Shutdown()

	err := s.connectionManager.StartListening()
	if err != nil {
		log.Errorf("Failed to start listening: %v", err)
		return err
	}

	for s.running {
		s.clientManager.ReapStaleClients()

		clientConnection, err := s.connectionManager.AcceptConnection()
		if err != nil {
			log.Errorf("Failed to accept connection: %v", err)
			break
		}

		clientSession := s.clientManager.AddClient(clientConnection, s.taskHandler)

		go clientSession.ProcessRequest()
	}

	return nil
}

func (s *Server) setupGracefulShutdown() {
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChannel
		log.Infof("action: shutdown_signal | result: received")
		s.Shutdown()
	}()
}

func (s *Server) Shutdown() {
	s.running = false
	s.taskHandler.Close()
	s.clientManager.Close()
	s.connectionManager.Close()
	log.Infof("action: shutdown | result: success")
}
