package server

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/internal/task_executor"
	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/common/worker/storage"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	messageHandler worker.MessageHandler
	heatbeatSender heartbeat.HeartBeatHandler
}

func InitServer(conf *config.Config) *Server {

	// initiate Queues and Exchanges
	aggregatorInputQueue := middleware.GetAggregatorQueue(conf.Address, conf.ID)
	joinerOutputQueue := middleware.GetJoinerQueue(conf.Address)
	finishExchange := middleware.GetFinishExchange(conf.Address, []string{string(enum.AggregatorWorker)}, conf.ID)

	// initiate internal components
	cacheService := storage.NewDiskMemoryStorage()

	aggregatorService := business.NewAggregatorService(cacheService)

	taskExecutor := task_executor.NewAggregatorExecutor(
		conf,
		aggregatorService,
		joinerOutputQueue,
	)

	taskHandler := worker.NewTaskHandler(taskExecutor, true)

	messageHandler := worker.NewMessageHandler(
		taskHandler,
		[]middleware.MessageMiddleware{aggregatorInputQueue},
		finishExchange,
	)

	return &Server{
		messageHandler: messageHandler,
		heatbeatSender: heartbeat.NewHeartBeatHandler(conf.Heartbeat.Host, conf.Heartbeat.Port, conf.Heartbeat.Interval),
	}
}

func (s *Server) Run() error {
	logger.Logger.Info("Starting aggregator server...")
	s.setupGracefulShutdown()

	err := s.heatbeatSender.StartSending()
	if err != nil {
		logger.Logger.Errorf("action: start_heartbeat_sender | result: failed | error: %s", err.Error())
	}

	// This is a blocking call, it will run until an error occurs or
	// the Close() method is called via a signal

	e := s.messageHandler.Start()
	if e != nil {
		logger.Logger.Errorf("Error starting message handler: %v", e)
		s.Shutdown()
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
	s.heatbeatSender.Close()
	logger.Logger.Debug("aggregator Worker server shut down successfully.")
}

func (s *Server) getLastState(cacheService storage.StorageService) map[string][]int32 {

	files := cacheService.GetAllFilesReferences()

	lastState := make(map[string][]int32)

	for _, file := range files {

		read_ch, err := cacheService.ReadAllData(file)
		if err != nil {
			logger.Logger.Warnf("[%s] file could not be opened: %s", file, err.Error())
			continue
		}
		sequenceNumbers := s.getSequenceNumbers(read_ch)
		lastState[file] = sequenceNumbers
		logger.Logger.Infof("[%s] file read successfully, %d sequence numbers found", file, len(sequenceNumbers))
	}
	return lastState
}

func (s *Server) getSequenceNumbers(read_ch chan []byte) []int32 {
	sequenceNumbers := []int32{}
	for dataEnvelope := range read_ch {
		protoData := &protocol.DataEnvelope{}
		err := proto.Unmarshal(dataEnvelope, protoData)
		if err != nil {
			logger.Logger.Warnf("Dirty Message found, ignoring: %v", err)
			continue
		}
		sequenceNumbers = append(sequenceNumbers, protoData.SequenceNumber)
	}
	return sequenceNumbers

}
