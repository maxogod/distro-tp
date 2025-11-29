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
	"github.com/maxogod/distro-tp/src/group_by/business"
	"github.com/maxogod/distro-tp/src/group_by/config"
	"github.com/maxogod/distro-tp/src/group_by/internal/task_executor"
)

type Server struct {
	messageHandler  worker.MessageHandler
	heartbeatSender heartbeat.HeartBeatHandler
}

func InitServer(conf *config.Config) *Server {
	// initiateOutputs
	groupByInputQueue := middleware.GetGroupByQueue(conf.Address)
	reducerOutputQueue := middleware.GetReducerQueue(conf.Address)
	finishExchange := middleware.GetFinishExchange(conf.Address, []string{string(enum.GroupbyWorker)})

	// initiate internal components
	service := business.NewGroupService()

	taskExecutor := task_executor.NewGroupExecutor(
		service,
		conf.Address,
		reducerOutputQueue,
	)

	taskHandler := worker.NewTaskHandler(taskExecutor, true)

	messageHandler := worker.NewMessageHandler(
		taskHandler,
		[]middleware.MessageMiddleware{groupByInputQueue},
		finishExchange,
	)

	return &Server{
		messageHandler:  messageHandler,
		heartbeatSender: heartbeat.NewHeartBeatHandler(conf.Heartbeat.Host, conf.Heartbeat.Port, conf.Heartbeat.Interval),
	}
}

func (s *Server) Run() error {
	logger.Logger.Info("Starting Group By server...")
	s.setupGracefulShutdown()

	err := s.heartbeatSender.StartSending()
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
	logger.Logger.Debug("Shutting down Group By Worker server...")
	err := s.messageHandler.Close()
	if err != nil {
		logger.Logger.Errorf("Error closing message handler: %v", err)
	}

	s.heartbeatSender.Close()
	logger.Logger.Debug("Group By Worker server shut down successfully.")
}
