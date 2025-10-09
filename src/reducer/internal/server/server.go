package server

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/reducer/business"
	"github.com/maxogod/distro-tp/src/reducer/config"
	"github.com/maxogod/distro-tp/src/reducer/internal/task_executor"
)

var log = logger.GetLogger()

type Server struct {
	messageHandler worker.MessageHandler
}

func InitServer(conf *config.Config) *Server {

	// initiateOutputs
	reducerInputQueue := middleware.GetReducerQueue(conf.Address)
	// TODO: change this to joiner queue
	// until we have a joiner, we will use the aggregator queue as output
	// since the joiner only rewrites the ID value, this is fine
	// for testing purposes
	joinerOutputQueue := middleware.GetAggregatorQueue(conf.Address)

	// initiate internal components
	service := business.NewReducerService()

	taskExecutor := task_executor.NewReducerExecutor(
		service,
		joinerOutputQueue,
	)

	taskHandler := worker.NewTaskHandler(taskExecutor)

	messageHandler := worker.NewMessageHandler(
		taskHandler,
		[]middleware.MessageMiddleware{reducerInputQueue},
		nil,
	)

	return &Server{
		messageHandler: messageHandler,
	}
}

func (s *Server) Run() error {
	log.Info("Starting Group By server...")
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
	log.Debug("Shutting down Group By Worker server...")
	err := s.messageHandler.Close()
	if err != nil {
		log.Errorf("Error closing message handler: %v", err)
	}

	log.Debug("Group By Worker server shut down successfully.")
}
