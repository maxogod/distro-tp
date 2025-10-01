package server

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/filter/business"
	"github.com/maxogod/distro-tp/src/filter/config"
	"github.com/maxogod/distro-tp/src/filter/internal/handler"
)

var log = logger.GetLogger()

const (
	FinishExchange       = "finish_exchange"
	FilterQueue          = "filter"
	CountQueue           = "count"
	SumQueue             = "sum"
	ProcessDataQueue     = "processed_data"
	NodeConnectionsQueue = "node_connections"
)

type Server struct {
	config         *config.Config
	isRunning      bool
	messageHandler *handler.MessageHandler
	taskHandler    *handler.TaskHandler
}

func InitServer(conf *config.Config) *Server {

	workerService := business.NewFilterService()

	// TODO CHANGE THIS TO BE EXCHANGE!!! with finish_exchange and filterqueue
	filterQueueMiddleware, err := middleware.NewQueueMiddleware(conf.Address, FilterQueue)
	if err != nil {
		log.Errorf("Failed to create filter queue middleware: %v", err)
		return nil
	}

	reduceCountQueueMiddleware, err := middleware.NewQueueMiddleware(conf.Address, CountQueue)
	if err != nil {
		log.Errorf("Failed to create count queue middleware: %v", err)
		return nil
	}

	reduceSumQueueMiddleware, err := middleware.NewQueueMiddleware(conf.Address, SumQueue)
	if err != nil {
		log.Errorf("Failed to create sum queue middleware: %v", err)
		return nil
	}

	processDataQueueMiddleware, err := middleware.NewQueueMiddleware(conf.Address, ProcessDataQueue)
	if err != nil {
		log.Errorf("Failed to create processed data queue middleware: %v", err)
		return nil
	}

	connectionControllerMiddleware, err := middleware.NewQueueMiddleware(conf.Address, NodeConnectionsQueue)
	if err != nil {
		log.Errorf("Failed to create controller connection middleware: %v", err)
		return nil
	}

	finishExchangeMiddleware, err := middleware.NewQueueMiddleware(conf.Address, FinishExchange)
	if err != nil {
		log.Errorf("Failed to create finish exchange middleware: %v", err)
		return nil
	}

	messageHandler := handler.NewMessageHandler(
		filterQueueMiddleware,
		reduceSumQueueMiddleware,
		reduceCountQueueMiddleware,
		processDataQueueMiddleware,
		connectionControllerMiddleware,
		finishExchangeMiddleware,
	)

	return &Server{
		config:         conf,
		isRunning:      true,
		messageHandler: messageHandler,
		taskHandler: handler.NewTaskHandler(
			workerService,
			messageHandler,
			&conf.TaskConfig),
	}
}

func (s *Server) Run() error {
	log.Info("Starting Filter server...")

	s.setupGracefulShutdown()
	defer s.Shutdown()

	e := s.messageHandler.AnnounceToController()
	if e != nil {
		log.Errorf("Failed to announce to controller: %v", e)
		return fmt.Errorf("failed to announce to controller: %v", e)
	}

	for s.isRunning {

		e := s.messageHandler.StartReceiving(func(payload []byte, taskType int32) error {
			return s.taskHandler.HandleTask(enum.TaskType(taskType), payload)
		})

		if e != middleware.MessageMiddlewareSuccess {
			log.Errorf("Failed to start consuming: %d", e)
			return fmt.Errorf("failed to start consuming: %d", e)
		}

		err := s.messageHandler.SendDone()

		if err != nil {
			log.Errorf("Failed to send done message: %v", err)
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
		log.Infof("action: shutdown_signal | result: received")
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
}
