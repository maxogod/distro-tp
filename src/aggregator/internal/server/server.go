package server

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	cache "github.com/maxogod/distro-tp/src/aggregator/cache/disk_memory"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/internal/task_executor"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/worker"
)

type Server struct {
	messageHandler worker.MessageHandler
}

func InitServer(conf *config.Config) *Server {

	// initiateOutputs
	aggregatorInputQueue := middleware.GetAggregatorQueue(conf.Address)
	finishExchange := middleware.GetFinishExchange(conf.Address, []string{string(enum.AggregatorWorker)})

	// initiate internal components
	cacheService := cache.NewDiskMemoryStorage()
	connectedClients := make(map[string]middleware.MessageMiddleware)

	aggregatorService := business.NewAggregatorService(cacheService)

	taskExecutor := task_executor.NewAggregatorExecutor(
		conf,
		connectedClients,
		aggregatorService,
	)

	taskHandler := worker.NewTaskHandler(taskExecutor)

	messageHandler := worker.NewMessageHandler(
		taskHandler,
		[]middleware.MessageMiddleware{aggregatorInputQueue},
		finishExchange,
	)

	return &Server{
		messageHandler: messageHandler,
	}
}

func (s *Server) Run() error {
	logger.Logger.Info("Starting aggregator server...")
	s.setupGracefulShutdown()

	// This is a blocking call, it will run until an error occurs or
	// the Close() method is called via a signal
	e := s.messageHandler.Start()
	if e != nil {
		logger.Logger.Errorf("Error starting message handler: %v", e)
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
		logger.Logger.Debugf("Shutdown Signal received, shutting down...")
		s.Shutdown()
	}()
}

func (s *Server) Shutdown() {
	logger.Logger.Debug("Shutting down aggregator Worker server...")
	err := s.messageHandler.Close()
	if err != nil {
		logger.Logger.Errorf("Error closing message handler: %v", err)
	}

	logger.Logger.Debug("aggregator Worker server shut down successfully.")
}
