package server

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/logger"
	service "github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/config"
)

var log = logger.GetLogger()

type Server struct {
	config        *config.Config
	isRunning     bool
	workerService *service.Joiner
}

func InitServer(config *config.Config) *Server {
	return &Server{
		config:        config,
		isRunning:     true,
		workerService: service.NewJoiner(config),
	}
}

func (s *Server) Run() error {
	log.Info("Starting Basic Worker server...")

	s.setupGracefulShutdown()
	defer s.Shutdown()

	err := s.workerService.InitService()
	if err != nil {
		return err
	}

	for s.isRunning {
	}

	err = s.workerService.Stop()
	if err != nil {
		log.Errorf("Error stopping worker service: %v", err)
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
