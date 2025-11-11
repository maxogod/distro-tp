package server

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/worker"
	disk_storage "github.com/maxogod/distro-tp/src/common/worker/storage/disk_memory"
	"github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/internal/task_executor"
)

type Server struct {
	messageHandler worker.MessageHandler
	heatbeatSender heartbeat.HeartBeatSender
}

func InitServer(conf *config.Config) *Server {
	// initiate middlewares
	joinerInputQueue := middleware.GetJoinerQueue(conf.Address)
	refDataExchange := middleware.GetRefDataExchange(conf.Address)
	finishExchange := middleware.GetFinishExchange(conf.Address, []string{string(enum.JoinerWorker)})

	// initiate internal components
	cacheService := cache.NewInMemoryCache()
	storageService := disk_storage.NewDiskMemoryStorage()

	joinerService := business.NewJoinerService(cacheService, storageService, conf.AmountOfUsersPerFile)

	taskExecutor := task_executor.NewJoinerExecutor(
		conf,
		joinerService,
	)

	taskHandler := worker.NewTaskHandler(taskExecutor, false)

	messageHandler := worker.NewMessageHandler(
		taskHandler,
		[]middleware.MessageMiddleware{joinerInputQueue, refDataExchange},
		finishExchange,
	)

	return &Server{
		messageHandler: messageHandler,
		heatbeatSender: heartbeat.NewHeartBeatSender(conf.Heartbeat.Host, conf.Heartbeat.Port, conf.Heartbeat.Interval),
	}
}

func (s *Server) Run() error {
	logger.Logger.Info("Starting joiner server...")
	s.setupGracefulShutdown()

	err := s.heatbeatSender.Start()
	if err != nil {
		logger.Logger.Errorf("action: start_heartbeat_sender | result: failed | error: %s", err.Error())
	}

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
	logger.Logger.Debug("Shutting down joiner Worker server...")
	err := s.messageHandler.Close()
	if err != nil {
		logger.Logger.Errorf("Error closing message handler: %v", err)
	}

	s.heatbeatSender.Close()
	logger.Logger.Debug("joiner Worker server shut down successfully.")
}
