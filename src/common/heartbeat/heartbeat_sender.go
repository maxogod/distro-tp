package heartbeat

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

type heartbeatSender struct {
	host     string
	port     int
	interval int

	ctx    context.Context
	cancel context.CancelFunc
	conn   *net.UDPConn
}

// NewHeartBeatSender creates a new instance of HeartBeatSender.
func NewHeartBeatSender(host string, port int, interval int) HeartBeatSender {
	ctx, cancel := context.WithCancel(context.Background())
	return &heartbeatSender{
		host:     host,
		port:     port,
		interval: interval,

		ctx:    ctx,
		cancel: cancel,
	}
}

func (h *heartbeatSender) Start() error {
	addr := fmt.Sprintf("%s:%d", h.host, h.port)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %w", err)
	}

	// No retries are needed, as udp is connectionless
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return fmt.Errorf("failed to dial UDP: %w", err)
	}
	h.conn = conn

	go h.sendAtIntervals()
	return nil
}

func (h *heartbeatSender) Close() {
	if h.cancel != nil {
		h.cancel()
	}

	if h.conn != nil {
		h.conn.Close()
	}
}

func (h *heartbeatSender) sendAtIntervals() {
	ticker := time.NewTicker(time.Duration(h.interval) * time.Second)
	defer ticker.Stop()

	if err := h.sendHeartbeat(); err != nil {
		logger.Logger.Errorf("Error sending initial heartbeat: %v\n", err)
	}

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			if err := h.sendHeartbeat(); err != nil {
				logger.Logger.Errorf("Error sending heartbeat: %v\n", err)
			}
		}
	}
}

func (h *heartbeatSender) sendHeartbeat() error {
	hb := &protocol.HeartBeat{
		Timestamp: time.Now().Unix(),
	}

	data, err := proto.Marshal(hb)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat: %w", err)
	}

	_, err = h.conn.Write(data)
	if err != nil {
		return fmt.Errorf("failed to send UDP packet: %w", err)
	}

	return nil
}
