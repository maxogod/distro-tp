package server

import (
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/controller/config"
	"github.com/maxogod/distro-tp/src/controller/internal/sessions/manager"
	"github.com/maxogod/distro-tp/src/controller/internal/storage"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	config        *config.Config
	running       atomic.Bool
	clientManager manager.ClientManager
	counterStore  storage.CounterStorage

	newClientsChan        chan *protocol.ControlMessage
	initControlMiddleware middleware.MessageMiddleware
	finishAcceptingChan   chan bool
	heartbeatSender       heartbeat.HeartBeatHandler
}

func NewServer(conf *config.Config) *Server {
	counterStore := storage.NewCounterStorage(conf.StoragePath)
	s := &Server{
		config:                conf,
		clientManager:         manager.NewClientManager(conf, counterStore),
		newClientsChan:        make(chan *protocol.ControlMessage, conf.MaxClients),
		initControlMiddleware: middleware.GetInitControlQueue(conf.MiddlewareAddress, conf.ID),
		finishAcceptingChan:   make(chan bool),
		heartbeatSender:       heartbeat.NewHeartBeatHandler(conf.Heartbeat.Host, conf.Heartbeat.Port, conf.Heartbeat.Interval),
		counterStore:          counterStore,
	}
	s.running.Store(true)

	go s.acceptNewClients()

	return s
}

func (s *Server) Run() error {
	s.setupGracefulShutdown()
	defer s.Shutdown()

	err := s.heartbeatSender.StartSending()
	if err != nil {
		logger.Logger.Errorf("action: start_heartbeat_sender | result: failed | error: %s", err.Error())
	}

	s.restoreClientsFromStorage()

	for controlMsg := range s.newClientsChan {
		if !s.running.Load() {
			break
		}

		s.clientManager.ReapStaleClients()

		clientSession := s.clientManager.AddClient(controlMsg.GetClientId(), enum.TaskType(controlMsg.GetTaskType()), nil)

		if controlMsg.GetIsAbort() {
			s.clientManager.RemoveClient(controlMsg.GetClientId())
			logger.Logger.Infof("action: remove_client | client_id: %s | result: success", controlMsg.GetClientId())
			continue
		}
		go func() {
			err = clientSession.InitiateControlSequence()
			if err != nil {
				logger.Logger.Debugf("action: initiate_control_sequence | result: failed | error: %s", err.Error())
			}
		}()

		logger.Logger.Infof("action: add_client | client_id: %s | result: success", controlMsg.GetClientId())
	}

	return nil
}

func (s *Server) Shutdown() {
	if !s.running.Load() {
		return
	}

	s.running.Store(false)
	s.clientManager.Close()
	s.finishAcceptingChan <- true // Stop accepting new clients
	s.initControlMiddleware.Close()

	s.heartbeatSender.Close()
	logger.Logger.Infof("action: shutdown | result: success")
}

/* --- PRIVATE UTILS --- */

func (s *Server) setupGracefulShutdown() {
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChannel
		logger.Logger.Infof("action: shutdown_signal | result: received")
		s.Shutdown()
	}()
}

func (s *Server) acceptNewClients() {
	done := make(chan bool)
	e := s.initControlMiddleware.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		running := true
		for running {
			var msg middleware.MessageDelivery
			select {
			case m := <-msgs:
				msg = m
			case <-s.finishAcceptingChan:
				running = false
				continue
			}

			controlMsg := protocol.ControlMessage{}
			err := proto.Unmarshal(msg.Body, &controlMsg)
			if err != nil {
				logger.Logger.Errorf("action: accept_new_clients | result: failed | error: %s", err.Error())
				msg.Nack(false, false)
				continue
			}

			if controlMsg.GetClientId() == "" {
				logger.Logger.Errorf("action: accept_new_clients | result: failed | error: client id cannot be empty")
				msg.Nack(false, false)
				continue
			}

			if controlMsg.GetControllerId() != 0 && controlMsg.GetControllerId() != s.config.NumericID {
				logger.Logger.Debugf(
					"action: accept_new_clients | client_id: %s | controller_id: %d | result: not_mine",
					controlMsg.GetClientId(),
					controlMsg.GetControllerId(),
				)
				msg.Nack(false, true)
				continue
			}

			if err = s.counterStore.InitializeClientCounter(controlMsg.GetClientId(), enum.TaskType(controlMsg.GetTaskType())); err != nil {
				logger.Logger.Debugf("action: ensure_client_storage | client_id: %s | result: failed | error: %s", controlMsg.GetClientId(), err.Error())
				msg.Nack(false, true)
				continue
			}

			s.newClientsChan <- &controlMsg

			msg.Ack(false)
		}
		done <- true
	})
	if e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("action: accept_new_clients | result: failed | error: %d", e)
	}
	<-done
	close(s.newClientsChan)
}

func (s *Server) restoreClientsFromStorage() {
	logger.Logger.Infof("action: restore_clients | result: started")

	clients, err := s.counterStore.GetClientIds()
	if err != nil {
		logger.Logger.Errorf("action: restore_clients | result: failed | error: %s", err.Error())
		return
	}

	for _, clientID := range clients {
		logger.Logger.Infof("action: restore_client | client_id: %s | result: started", clientID)

		counters, readErr := s.counterStore.ReadClientCounters(clientID)
		if readErr != nil {
			logger.Logger.Errorf("action: restore_client | client_id: %s | result: failed", clientID)
			if counters == nil {
				deleteErr := s.counterStore.RemoveClient(clientID)
				if deleteErr != nil {
					logger.Logger.Errorf("action: delete_client | client_id: %s | result: failed | error: %s", clientID, deleteErr.Error())
				}
			}
		}

		if len(counters) == 0 {
			logger.Logger.Infof("action: restore_client | client_id: %s | result: no_counters", clientID)
			deleteErr := s.counterStore.RemoveClient(clientID)
			if deleteErr != nil {
				logger.Logger.Errorf("action: delete_client | client_id: %s | result: failed | error: %s", clientID, deleteErr.Error())
			}
			continue
		}

		taskType := enum.TaskType(counters[0].GetTaskType())

		logger.Logger.Debugf("action: restore_client | client_id: %s | result: task_type: %d", clientID, taskType)
		// Remove the first counter from the list, as it is the task type
		var clientCounters = counters
		if len(counters) > 1 {
			clientCounters = counters[1:]
		} else {
			clientCounters = nil
		}
		s.clientManager.AddClient(clientID, taskType, clientCounters)
		logger.Logger.Infof("action: restore_client | client_id: %s | result: success", clientID)
	}
}
