package server

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/internal/handler"
)

var log = logger.GetLogger()

type Server struct {
	config         *config.Config
	isRunning      bool
	messageHandler *handler.MessageHandler
	taskHandler    *handler.TaskHandler
}

func InitServer(conf *config.Config) *Server {

	joinerService := business.NewFilterService()

	messageHandler := handler.NewMessageHandler(
		conf.Address,
	)

	return &Server{
		config:         conf,
		isRunning:      true,
		messageHandler: messageHandler,
		taskHandler: handler.NewTaskHandler(
			joinerService,
			messageHandler,
			&conf.TaskConfig),
	}
}

func (s *Server) Run() error {
	log.Info("Starting Filter server...")

	s.setupGracefulShutdown()

	e := s.messageHandler.AnnounceToController()
	if e != nil {
		log.Errorf("Failed to announce to controller: %v", e)
		return fmt.Errorf("failed to announce to controller: %v", e)
	}

	for s.isRunning {

		e := s.messageHandler.Start(
			func(payload []byte, taskType int32) error {
				return s.taskHandler.HandleTask(enum.TaskType(taskType), payload)
			},
			func(payload []byte, taskType int32) error { // TODO: replace with reference handler
				return s.taskHandler.HandleReferenceTask(enum.TaskType(taskType), payload)
			},
		)

		if e != nil {
			log.Errorf("Failed to start consuming: %d", e)
			s.Shutdown()
			return fmt.Errorf("failed to start consuming: %d", e)
		}

		if !s.isRunning {
			// Hot-fix to avoid
			// sending done message twice in case of shutdown signal
			break
		}

		err := s.messageHandler.SendDone()

		log.Debug("Sent done message to controller")

		if err != nil {
			log.Errorf("Failed to send done message: %v", err)
			s.Shutdown()
			return fmt.Errorf("failed to send done message: %v", err)
		}

	}

	return nil
}

func (s *Server) setupGracefulShutdown() {
	// This is a graceful non-blocking setup to shut down the process in case
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChannel
		log.Infof("Shutdown Signal received, shutting down...")
		s.Shutdown()
	}()
}

func (s *Server) Shutdown() {
	log.Debug("Shutting down Filter Worker server...")
	s.isRunning = false
	err := s.messageHandler.Close()
	if err != nil {
		log.Errorf("Error closing message handler: %v", err)
	}
	log.Debug("Filter Worker server shut down successfully.")
}
