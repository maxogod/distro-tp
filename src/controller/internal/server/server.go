package server

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/controller/config"
	"github.com/maxogod/distro-tp/src/controller/internal/sessions/manager"
)

var log = logger.GetLogger()

type Server struct {
	config        *config.Config
	running       bool
	clientManager manager.ClientManager
}

func NewServer(conf *config.Config) *Server {
	return &Server{
		config:        conf,
		running:       true,
		clientManager: manager.NewClientManager(conf),
	}
}

func (s *Server) Run() error {
	s.setupGracefulShutdown()
	defer s.Shutdown()

	for s.running {

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

	log.Infof("action: shutdown | result: success")
}

/* --- PRIVATE UTILS --- */

func (s *Server) acceptNewClients() {}
