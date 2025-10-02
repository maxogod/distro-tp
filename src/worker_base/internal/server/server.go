package server

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/worker_base/business"
	"github.com/maxogod/distro-tp/src/worker_base/config"
)

var log = logger.GetLogger()

type Server struct {
	config        *config.Config
	isRunning     bool
	workerService *business.WorkerService
}

func InitServer(conf *config.Config) *Server {
	return &Server{
		config:        conf,
		isRunning:     true,
		workerService: business.NewWorkerService(),
	}
}

func (s *Server) Run() error {
	log.Info("Starting Basic Worker server...")

	s.setupGracefulShutdown()
	defer s.Shutdown()

	for s.isRunning {

		time.Sleep(1 * time.Second)
		s.workerService.HandleTask()

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
