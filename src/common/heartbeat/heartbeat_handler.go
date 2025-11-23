package heartbeat

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

const BUFFER_SIZE = 1024
const MAX_CONNECTION_RETRIES = 5
const CONNECTION_RETRY_DELAY = 1

type heartbeatHandler struct {
	host     string
	port     int
	interval int

	conn *net.UDPConn

	ctx    context.Context
	cancel context.CancelFunc
}

// NewHeartBeatHandler creates a new instance of HeartBeatHandler.
// The host and port specify the address to receive heartbeats from or send heartbeats to
func NewHeartBeatHandler(host string, port int, interval int) HeartBeatHandler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &heartbeatHandler{
		host:     host,
		port:     port,
		interval: interval,

		ctx:    ctx,
		cancel: cancel,
	}
	return h
}

func NewListeningHeartBeatHandler(host string, port int, interval int) HeartBeatHandler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &heartbeatHandler{
		host:     host,
		port:     port,
		interval: interval,

		ctx:    ctx,
		cancel: cancel,
	}
	h.startListening(host, port)
	return h
}

func (h *heartbeatHandler) StartSending() error {

	addr := fmt.Sprintf("%s:%d", h.host, h.port)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %w", err)
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return fmt.Errorf("failed to dial UDP: %w", err)
	}
	go h.sendAtIntervals(conn)
	return nil
}

func (h *heartbeatHandler) StartSendingToAll(destinationAddrs []string) error {

	for _, addr := range destinationAddrs {
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return fmt.Errorf("failed to resolve UDP address: %w", err)
		}
		conn, err := net.DialUDP("udp", nil, udpAddr)
		if err != nil {
			continue
		}
		go h.sendAtIntervals(conn)
	}
	return nil
}

func (h *heartbeatHandler) StartReceiving(onTimeoutFunc func(amountOfHeartbeats int), timeoutAmount int) error {
	go h.receiveHeartbeatsWithTimeout(onTimeoutFunc, timeoutAmount)
	return nil
}

func (h *heartbeatHandler) Stop() {
	if h.cancel != nil {
		h.cancel()
		h.ctx, h.cancel = context.WithCancel(context.Background())
	}
}

func (h *heartbeatHandler) Close() {
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
	if h.conn != nil {
		h.conn.Close()
		h.conn = nil
	}
}

// ------------ Private Methods ------------

func (h *heartbeatHandler) startListening(host string, port int) error {
	h.host = host
	h.port = port

	addr := fmt.Sprintf("%s:%d", h.host, h.port)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	h.conn = conn
	return nil
}

func (h *heartbeatHandler) sendAtIntervals(conn *net.UDPConn) {
	ticker := time.NewTicker(time.Duration(h.interval) * time.Second)
	defer ticker.Stop()

	if err := h.sendHeartbeat(conn); err != nil {
		logger.Logger.Errorf("Error sending initial heartbeat: %v\n", err)
	}

	for {
		select {
		case <-h.ctx.Done():
			conn.Close()
			return
		case <-ticker.C:
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

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("failed to send UDP packet: %w", err)
	}

	return nil
}

func (h *heartbeatHandler) receiveHeartbeatsWithTimeout(onTimeoutFunc func(amountOfHeartbeats int), timeoutAmount int) {
	buf := make([]byte, BUFFER_SIZE)
	var heartbeatCounter atomic.Int64

	timeoutTimer := time.NewTimer(time.Duration(timeoutAmount) * time.Second)
	defer timeoutTimer.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-timeoutTimer.C:
			onTimeoutFunc(int(heartbeatCounter.Load()))
			return
		default:
			h.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)) // This is to not block indefinitely
			n, _, err := h.conn.ReadFromUDP(buf)

			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				logger.Logger.Errorf("Error receiving heartbeat: %v", err)
				return
			}
			// Process heartbeat
			var hb protocol.HeartBeat
			if err := proto.Unmarshal(buf[:n], &hb); err != nil {
				continue
			}
			heartbeatCounter.Add(1)
			timeoutTimer.Reset(time.Duration(timeoutAmount) * time.Second)
		}
	}
}
