package server

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/gateway_controller/config"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/healthcheck"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/network"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/sessions/manager"
)

var log = logger.GetLogger()

type Server struct {
	config            *config.Config
	running           bool
	connectionManager network.ConnectionManager
	clientManager     manager.ClientManager
	pingServer        healthcheck.PingServer // for service health checks
}

func NewServer(conf *config.Config) *Server {
	return &Server{
		config:            conf,
		running:           true,
		connectionManager: network.NewConnectionManager(conf.Port),
		clientManager:     manager.NewClientManager(conf),
		pingServer:        healthcheck.NewPingServer(int(conf.HealthCheckPort)), // for health check
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

	go s.pingServer.Run() // Start health check server

	for s.running {
		s.clientManager.ReapStaleClients()

		clientConnection, err := s.connectionManager.AcceptConnection()
		if err != nil {
			log.Errorf("Failed to accept connection: %v", err)
			break
		}

		clientSession := s.clientManager.AddClient(clientConnection)

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
	s.clientManager.Close()
	s.connectionManager.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.pingServer.Shutdown(ctx)

	log.Infof("action: shutdown | result: success")
}
