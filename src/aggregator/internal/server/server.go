package server

import (
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/internal/task_executor"
	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/leader_election"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/maxogod/distro-tp/src/common/worker/storage"
)

type Server struct {
	iAmLeader      atomic.Bool
	messageHandler worker.MessageHandler
	heatbeatSender heartbeat.HeartBeatHandler
	leaderElection leader_election.LeaderElection
}

func InitServer(conf *config.Config) *Server {

	// initiate Queues and Exchanges
	aggregatorInputQueue := middleware.GetAggregatorQueue(conf.Address)
	joinerOutputQueue := middleware.GetJoinerQueue(conf.Address)
	finishExchange := middleware.GetFinishExchange(conf.Address, []string{string(enum.AggregatorWorker)})
	//leaderElectionUpdateHandler := update_handler.NewUpdateHandler()
	leaderElection := leader_election.NewLeaderElection(
		conf.LeaderElection.Host,
		conf.LeaderElection.Port,
		int32(conf.LeaderElection.ID),
		conf.Address,
		enum.AggregatorWorker,
		conf.LeaderElection.MaxNodes,
		nil, //leaderElectionUpdateHandler
	)

	// initiate internal components
	cacheService := storage.NewDiskMemoryStorage()
	connectedClients := make(map[string]middleware.MessageMiddleware)

	aggregatorService := business.NewAggregatorService(cacheService)

	taskExecutor := task_executor.NewAggregatorExecutor(
		conf,
		connectedClients,
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
		leaderElection: leaderElection,
		iAmLeader:      atomic.Bool{},
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

	go func() {
		e := s.messageHandler.Start()
		if e != nil {
			logger.Logger.Errorf("Error starting message handler: %v", e)
			s.Shutdown()
			return
		}
	}()

	s.leaderElection.Start()

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
	s.leaderElection.Close()
	logger.Logger.Debug("aggregator Worker server shut down successfully.")
}
