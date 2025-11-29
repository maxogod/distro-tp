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
	"github.com/maxogod/distro-tp/src/filter/business"
	"github.com/maxogod/distro-tp/src/filter/config"
	"github.com/maxogod/distro-tp/src/filter/internal/task_executor"
)

type Server struct {
	messageHandler  worker.MessageHandler
	heartbeatSender heartbeat.HeartBeatHandler
}

func InitServer(conf *config.Config) *Server {
	// initiateOutputs
	filterInputQueue := middleware.GetFilterQueue(conf.Address)
	groupByOutputQueue := middleware.GetGroupByQueue(conf.Address)

	// initiate internal components
	filterService := business.NewFilterService(
		conf.TaskConfig.FilterYearFrom,
		conf.TaskConfig.FilterYearTo,
		conf.TaskConfig.BusinessHourFrom,
		conf.TaskConfig.BusinessHourTo,
		conf.TaskConfig.TotalAmountThreshold,
	)
	connectedClients := make(map[string]middleware.MessageMiddleware)
	processedOutputQueue := make(map[string]middleware.MessageMiddleware)
	finishExchange := middleware.GetFinishExchange(conf.Address, []string{string(enum.FilterWorker)})

	taskExecutor := task_executor.NewFilterExecutor(
		conf.TaskConfig,
		conf.Address,
		filterService,
		connectedClients,
		groupByOutputQueue,
		processedOutputQueue, // To be used in T1
	)

	taskHandler := worker.NewTaskHandler(taskExecutor, true)

	messageHandler := worker.NewMessageHandler(
		taskHandler,
		[]middleware.MessageMiddleware{filterInputQueue},
		finishExchange,
	)

	return &Server{
		messageHandler:  messageHandler,
		heartbeatSender: heartbeat.NewHeartBeatHandler(conf.Heartbeat.Host, conf.Heartbeat.Port, conf.Heartbeat.Interval),
	}
}

func (s *Server) Run() error {
	logger.Logger.Info("Starting Filter server...")
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
	logger.Logger.Debug("Shutting down Filter Worker server...")
	err := s.messageHandler.Close()
	if err != nil {
		logger.Logger.Errorf("Error closing message handler: %v", err)
	}

	s.heartbeatSender.Close()
	logger.Logger.Debug("Filter Worker server shut down successfully.")
}
