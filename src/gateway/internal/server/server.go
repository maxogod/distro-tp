package server

import (
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/leader_election"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
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
	leaderElection    leader_election.LeaderElection
}

func NewServer(conf *config.Config) *Server {
	leaderElection := leader_election.NewLeaderElection(
		conf.LeaderElection.Host,
		conf.LeaderElection.Port,
		int32(conf.LeaderElection.ID),
		conf.MiddlewareAddress,
		enum.Gateway,
		conf.LeaderElection.MaxNodes,
		nil, // updateCallbacks
	)

	s := &Server{
		config:            conf,
		connectionManager: network.NewConnectionManager(conf.Port),
		clientManager:     manager.NewClientManager(conf),
		pingServer:        healthcheck.NewPingServer(int(conf.HealthCheckPort)), // for health check
		heartbeatSender:   heartbeat.NewHeartBeatHandler(conf.Heartbeat.Host, conf.Heartbeat.Port, conf.Heartbeat.Interval),
		leaderElection:    leaderElection,
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

	go func() {
		err = s.leaderElection.Start()
		if err != nil {
			logger.Logger.Errorf("Failed to start leader election: %v", err)
		}
	}()

	for s.running.Load() {
		s.clientManager.ReapStaleClients()

		clientConnection, err := s.connectionManager.AcceptConnection()
		if err != nil {
			logger.Logger.Errorf("Failed to accept connection: %v", err)
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
