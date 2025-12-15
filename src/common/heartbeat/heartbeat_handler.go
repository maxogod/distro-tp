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

const BUFFER_SIZE = 1024

type heartbeatHandler struct {
	host     string
	port     int
	interval time.Duration
	amount   int

	ctx    context.Context
	cancel context.CancelFunc
}

// NewHeartBeatHandler creates a new instance of HeartBeatHandler.
// The host and port specify the address to receive heartbeats from or send heartbeats to
func NewHeartBeatHandler(host string, port int, interval time.Duration, amount int) HeartBeatHandler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &heartbeatHandler{
		host:     host,
		port:     port,
		interval: interval,
		amount:   amount,

		ctx:    ctx,
		cancel: cancel,
	}
	return h
}

func (h *heartbeatHandler) StartSending() error {

	for i := range h.amount {
		addr := fmt.Sprintf("%s%d:%d", h.host, i+1, h.port)
		go h.sendAtIntervals(addr)
	}
	return nil
}

func (h *heartbeatHandler) Close() {
	if h.cancel != nil {
		h.cancel()
	}
}

// ------------ Private Methods ------------

func (h *heartbeatHandler) sendAtIntervals(addr string) {
	sendTicker := time.NewTicker(h.interval)
	defer sendTicker.Stop()
	resolutionTicker := time.NewTicker(2 * time.Second)
	defer resolutionTicker.Stop()

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		logger.Logger.Errorf("Error resolving addr: %v\n", err)
		return
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		logger.Logger.Errorf("Error dialing: %v\n", err)
		return
	}
	defer conn.Close()

	if err := h.sendHeartbeat(conn); err != nil {
		logger.Logger.Errorf("Error sending initial heartbeat: %v\n", err)
	}

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-resolutionTicker.C:
			newUdpAddr, err := net.ResolveUDPAddr("udp", addr)
			if err != nil {
				continue
			}
			if newUdpAddr.IP.String() != udpAddr.IP.String() || newUdpAddr.Port != udpAddr.Port {
				conn.Close()
				conn, err = net.DialUDP("udp", nil, newUdpAddr)
				if err != nil {
					continue
				}
				udpAddr = newUdpAddr
			}
		case <-sendTicker.C:
			if err := h.sendHeartbeat(conn); err != nil {
				logger.Logger.Errorf("Error sending heartbeat: %v\n", err)
			}
		}
	}
}

func (h *heartbeatHandler) sendHeartbeat(conn *net.UDPConn) error {
	hb := &protocol.HeartBeat{
		Timestamp: time.Now().Unix(),
	}

	data, err := proto.Marshal(hb)
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat: %w", err)
	}

	conn.Write(data)

	return nil
}
