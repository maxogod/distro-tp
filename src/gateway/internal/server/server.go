package server

import (
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/logger"
	commonNetwork "github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/config"
	"github.com/maxogod/distro-tp/src/gateway/internal/healthcheck"
	"github.com/maxogod/distro-tp/src/gateway/internal/network"
	"github.com/maxogod/distro-tp/src/gateway/internal/sessions/manager"
)

type Server struct {
	config            *config.Config
	running           atomic.Bool
	connectionManager network.ConnectionManager
	clientManager     manager.ClientManager
	pingServer        healthcheck.PingServer // for service health checks
	heartbeatSender   heartbeat.HeartBeatHandler
}

func NewServer(conf *config.Config) *Server {
	s := &Server{
		config:            conf,
		connectionManager: network.NewConnectionManager(conf.Port),
		clientManager:     manager.NewClientManager(conf),
		pingServer:        healthcheck.NewPingServer(int(conf.HealthCheckPort)), // for health check
		heartbeatSender:   heartbeat.NewHeartBeatHandler(conf.Heartbeat.Host, conf.Heartbeat.Port, conf.Heartbeat.Interval),
	}
	s.running.Store(true)

	return s
}

func (s *Server) Run() error {
	s.setupGracefulShutdown()
	defer s.Shutdown()

	err := s.connectionManager.StartListening()
	if err != nil {
		logger.Logger.Errorf("Failed to start listening: %v", err)
		return err
	}

	go s.pingServer.Run() // Start health check server
	err = s.heartbeatSender.StartSending()
	if err != nil {
		logger.Logger.Errorf("action: start_heartbeat_sender | result: failed | error: %s", err.Error())
	}

	for s.running.Load() {
		s.clientManager.ReapStaleClients()

		clientConnection, connErr := s.connectionManager.AcceptConnection()
		if connErr != nil {
			if !s.running.Load() {
				logger.Logger.Infof("action: shutdown_signal | result: closing listener")
				break
			}
			logger.Logger.Errorf("Failed to accept connection: %v", connErr)
			break
		}

		go func(conn commonNetwork.ConnectionInterface) {
			clientSession := s.clientManager.AddClient(conn)
			if clientSession == nil {
				closeErr := conn.Close()
				if closeErr != nil {
					logger.Logger.Errorf("Failed to close connection: %v", closeErr)
					return
				}
				return
			}

			clientSession.ProcessRequest()
		}(clientConnection)
	}

	return nil
}

func (s *Server) setupGracefulShutdown() {
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChannel
		logger.Logger.Infof("action: shutdown_signal | result: received")
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

	s.heartbeatSender.Close()
	logger.Logger.Infof("action: shutdown | result: success")
}
