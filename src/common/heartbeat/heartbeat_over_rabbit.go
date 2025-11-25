package heartbeat

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

type rabbitHeartbeatHandler struct {
	running             atomic.Bool
	heartbeatMiddleware middleware.MessageMiddleware
	interval            time.Duration
	serviceTimestamps   sync.Map

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRabbitHeartBeatHandler acts as a heartbeat sender or receiver
// over the corresponding rabbit middleware.
func NewRabbitHeartBeatHandler(middlewareUrl string, interval time.Duration) HeartBeatHandler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &rabbitHeartbeatHandler{
		heartbeatMiddleware: middleware.GetHeartbeatQueue(middlewareUrl),
		interval:            interval,
		ctx:                 ctx,
		cancel:              cancel,
	}
	return h
}

func (h *rabbitHeartbeatHandler) StartSending() error {
	h.running.Store(true)
	go h.sendAtIntervals()
	return nil
}

func (h *rabbitHeartbeatHandler) StartReceiving(onTimeoutFunc func(amountOfHeartbeats int), timeoutAmount time.Duration) error {
	h.running.Store(true)
	go h.receiveHeartbeatsWithTimeout(onTimeoutFunc, timeoutAmount)
	return nil
}

func (h *rabbitHeartbeatHandler) Close() {
	h.running.Store(false)
	if h.cancel != nil {
		h.cancel()
	}

	if err := h.heartbeatMiddleware.Close(); err != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("Error closing heartbeat middleware: %v", err)
	}
}

func (h *rabbitHeartbeatHandler) Stop() {
	h.running.Store(false)
	if h.cancel != nil {
		h.cancel()
		h.ctx, h.cancel = context.WithCancel(context.Background())
	}
}

func (h *rabbitHeartbeatHandler) StartSendingToAll(destinationAddrs []string) error {
	logger.Logger.Warn("Rabbit heartbeat handler doesnt support StartSendingToAll!")
	return nil
}

/* --- UTILS --- */

func (h *rabbitHeartbeatHandler) sendAtIntervals() {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	h.sendHeartbeat()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.sendHeartbeat()
		}
	}
}

func (h *rabbitHeartbeatHandler) sendHeartbeat() {
	hb := &protocol.HeartBeat{
		Timestamp: time.Now().Unix(),
	}

	data, err := proto.Marshal(hb)
	if err != nil {
		logger.Logger.Errorf("Error marshalling heartbeat: %v", err)
	}

	h.heartbeatMiddleware.Send(data)
}

func (h *rabbitHeartbeatHandler) receiveHeartbeatsWithTimeout(onTimeoutFunc func(amountOfHeartbeats int), timeoutAmount time.Duration) {
	hbCh := make(chan *protocol.HeartBeat)
	routineReadyCh := make(chan bool)

	go h.listenForHeartbeats(hbCh, routineReadyCh)
	<-routineReadyCh

	go h.checkTimeouts(timeoutAmount, onTimeoutFunc)

	for h.running.Load() {
		select {
		case <-h.ctx.Done():
			return
		case hb := <-hbCh:
			serviceName := hb.GetServiceName()
			if serviceName == "" {
				logger.Logger.Warn("Received heartbeat with empty service name")
				continue
			}

			now := time.Now()
			h.serviceTimestamps.Store(serviceName, now)
		}
	}
}

func (h *rabbitHeartbeatHandler) checkTimeouts(timeoutAmount time.Duration, onTimeoutFunc func(amountOfHeartbeats int)) {
	ticker := time.NewTicker(timeoutAmount / 2) // Check at half the timeout interval
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			timedOutCount := 0

			h.serviceTimestamps.Range(func(key, value any) bool {
				serviceName := key.(string)
				lastSeen := value.(time.Time)

				if now.Sub(lastSeen) > timeoutAmount {
					logger.Logger.Warnf("Service %s timed out (last seen: %v)", serviceName, lastSeen)
					h.serviceTimestamps.Delete(serviceName)
					timedOutCount++
				}
				return true
			})

			if timedOutCount > 0 {
				onTimeoutFunc(timedOutCount)
			}
		}
	}
}

func (h *rabbitHeartbeatHandler) listenForHeartbeats(hbCh chan *protocol.HeartBeat, routineReadyCh chan bool) {
	defer h.heartbeatMiddleware.StopConsuming()

	e := h.heartbeatMiddleware.StartConsuming(func(msgs middleware.ConsumeChannel, d chan error) {
		logger.Logger.Debugf("Started consuming from heartbeat exchange")
		routineReadyCh <- true
		receiving := true

		for receiving {
			var msg middleware.MessageDelivery
			select {
			case m := <-msgs:
				msg = m
			case <-h.ctx.Done():
				receiving = false
				continue
			}

			hb := &protocol.HeartBeat{}
			err := proto.Unmarshal(msg.Body, hb)
			if err != nil {
				logger.Logger.Errorf("Error unmarshaling heartbeat: %v", err)
				msg.Nack(false, false)
				continue
			}

			hbCh <- hb

			msg.Ack(false)
		}
	})
	if e != middleware.MessageMiddlewareSuccess {
		logger.Logger.Errorf("Error starting heartbeat listener: %d", e)
	}
}
