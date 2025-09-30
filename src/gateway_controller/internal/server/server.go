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
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/session"
)

var log = logger.GetLogger()

type Server struct {
	config            *config.Config
	isRunning         bool
	workerService     *business.GatewayControllerService
	taskHandler       *handler.TaskHandler
	connectionManager *network.ConnectionManager
	clientManager     *session.ClientManager
}

func InitServer(conf *config.Config) *Server {
	return &Server{
		config:            conf,
		isRunning:         true,
		taskHandler:       handler.NewTaskHandler(business.NewControllerService()),
		connectionManager: network.NewConnectionManager(conf.Port),
		clientManager:     session.NewClientManager(),
	}
}

func (s *Server) Run() error {
	log.Info("Starting Basic Worker server...")

	s.setupGracefulShutdown()
	defer s.Shutdown()

	err := s.connectionManager.StartListening()
	if err != nil {
		log.Errorf("Failed to start listening: %v", err)
		return err
	}

	for s.isRunning {

		clientConnection, err := s.connectionManager.AcceptConnection()
		if err != nil {
			log.Errorf("Failed to accept connection: %v", err)
			continue
		}

		clientSession := s.clientManager.AddClient(clientConnection, s.taskHandler)

		err = clientSession.ProcessRequest()

		if err != nil {
			log.Errorf("Error handling client request: %v", err)
			return err
		}

		err = clientSession.Close()
		if err != nil {
			log.Errorf("Error closing client session: %v", err)
		}

		s.clientManager.RemoveClient(clientSession.Id)

	}

	log.Info("Server shutdown complete")
	return nil
}

func (s *Server) setupGracefulShutdown() {
	// This is a graceful non-blocking setup to shut down the process in case
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChannel
		log.Infof("action: shutdown_signal | result: received")
		s.Shutdown()
	}()
}

func (s *Server) Shutdown() {
	s.isRunning = false
	log.Infof("action: shutdown | result: success")
}
