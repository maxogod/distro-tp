package server

import (
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/controller/config"
	"github.com/maxogod/distro-tp/src/controller/internal/sessions/manager"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	config        *config.Config
	running       atomic.Bool
	clientManager manager.ClientManager

	newClientsChan        chan string
	initControlMiddleware middleware.MessageMiddleware
	finishAcceptingChan   chan bool
	heartbeatSender       heartbeat.HeartBeatHandler
}

func NewServer(conf *config.Config) *Server {
	s := &Server{
		config:                conf,
		clientManager:         manager.NewClientManager(conf),
		newClientsChan:        make(chan string, conf.MaxClients),
		initControlMiddleware: middleware.GetInitControlQueue(conf.MiddlewareAddress),
		finishAcceptingChan:   make(chan bool),
		heartbeatSender:       heartbeat.NewHeartBeatHandler(conf.Heartbeat.Host, conf.Heartbeat.Port, conf.Heartbeat.Interval),
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

	for clientID := range s.newClientsChan {
		if !s.running.Load() {
			break
		}

		s.clientManager.ReapStaleClients()

		clientSession := s.clientManager.AddClient(clientID)
		go clientSession.InitiateControlSequence()

		logger.Logger.Infof("action: add_client | client_id: %s | result: success", clientID)
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

			s.newClientsChan <- controlMsg.GetClientId()

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
