package server

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maxogod/github.com/maxogod/distro-tp/src/filter/business"
	"github.com/maxogod/github.com/maxogod/distro-tp/src/filter/config"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Server struct {
	config        *config.Config
	isRunning     bool
	workerService *business.FilterService
}

func InitServer(conf *config.Config) *Server {
	return &Server{
		config:        conf,
		isRunning:     true,
		workerService: business.NewFilterService(),
	}
}

//TODO: protobuffs are necesary to implement here before adding server logic

func (s *Server) Run() error {
	log.Info("Starting Basic Worker server...")

	s.setupGracefulShutdown()
	defer s.Shutdown()

	for s.isRunning {

		time.Sleep(1 * time.Second)
		// Here would be the logic to fetch and process tasks
		// TODO: IMPLEMENT HERE!!!

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
