package server

import (
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/gateway/config"
	"github.com/maxogod/distro-tp/src/gateway/internal/healthcheck"
	"github.com/maxogod/distro-tp/src/gateway/internal/network"
	"github.com/maxogod/distro-tp/src/gateway/internal/sessions/manager"
)

var log = logger.GetLogger()

type Server struct {
	config            *config.Config
	running           atomic.Bool
	connectionManager network.ConnectionManager
	clientManager     manager.ClientManager
	pingServer        healthcheck.PingServer // for service health checks
}

func NewServer(conf *config.Config) *Server {
	s := &Server{
		config:            conf,
		connectionManager: network.NewConnectionManager(conf.Port),
		clientManager:     manager.NewClientManager(conf),
		pingServer:        healthcheck.NewPingServer(int(conf.HealthCheckPort)), // for health check
	}
	s.running.Store(true)

	return s
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

	for s.running.Load() {
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
	s.running.Store(false)
	s.clientManager.Close()
	s.connectionManager.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.pingServer.Shutdown(ctx)

	log.Infof("action: shutdown | result: success")
}
