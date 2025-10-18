package server

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/filter/business"
	"github.com/maxogod/distro-tp/src/filter/config"
	"github.com/maxogod/distro-tp/src/filter/internal/task_executor"
)

var log = logger.GetLogger()

type Server struct {
	messageHandler worker.MessageHandler
}

func InitServer(conf *config.Config) *Server {
	// initiateOutputs
	filterInputQueue := middleware.GetFilterQueue(conf.Address)
	aggregatorOutputQueue := middleware.GetAggregatorQueue(conf.Address)
	groupByOutputQueue := middleware.GetGroupByQueue(conf.Address)

	// initiate internal components
	filterService := business.NewFilterService()
	connectedClients := make(map[string]middleware.MessageMiddleware)

	taskExecutor := task_executor.NewFilterExecutor(
		conf.TaskConfig,
		conf.Address,
		filterService,
		connectedClients,
		groupByOutputQueue,
		aggregatorOutputQueue,
	)

	taskHandler := worker.NewTaskHandler(taskExecutor)

	messageHandler := worker.NewMessageHandler(
		taskHandler,
		[]middleware.MessageMiddleware{filterInputQueue},
		nil,
	)

	return &Server{
		messageHandler: messageHandler,
	}
}

func (s *Server) Run() error {
	log.Info("Starting Filter server...")
	s.setupGracefulShutdown()

	// This is a blocking call, it will run until an error occurs or
	// the Close() method is called via a signal
	e := s.messageHandler.Start()
	if e != nil {
		log.Errorf("Error starting message handler: %v", e)
		s.Shutdown()
		return e
	}
	return nil
}

func (s *Server) setupGracefulShutdown() {
	// This is a graceful non-blocking setup to shut down the process in case
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChannel
		log.Debugf("Shutdown Signal received, shutting down...")
		s.Shutdown()
	}()
}

func (s *Server) Shutdown() {
	log.Debug("Shutting down Filter Worker server...")
	err := s.messageHandler.Close()
	if err != nil {
		log.Errorf("Error closing message handler: %v", err)
	}

	log.Debug("Filter Worker server shut down successfully.")
}
