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
const MAX_CONNECTION_RETRIES = 5
const CONNECTION_RETRY_DELAY = 1

type heartbeatHandler struct {
	host     string
	port     int
	interval int

	ctx    context.Context
	cancel context.CancelFunc
}

// NewHeartBeatHandler creates a new instance of HeartBeatHandler.
// The single receiver is in charge of monitoring the multiple sender nodes
func NewHeartBeatHandler(host string, port int, interval int) HeartBeatHandler {
	ctx, cancel := context.WithCancel(context.Background())
	return &heartbeatHandler{
		host:     host,
		port:     port,
		interval: interval,

		ctx:    ctx,
		cancel: cancel,
	}
}

// NewReverseHeartBeatHandler creates a new instance of HeartBeatHandler.
// The multiple receivers are in charge of monitoring the single sender node
func NewReverseHeartBeatHandler(interval int) HeartBeatHandler {
	ctx, cancel := context.WithCancel(context.Background())
	return &heartbeatHandler{
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (h *heartbeatHandler) StartSending() error {
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
	//h.conn = conn
	go h.sendAtIntervals(conn)
	return nil
}

func (h *heartbeatHandler) StartSendingToAll(destinationAddrs []string, connectionRetries int) error {
	for _, addr := range destinationAddrs {
		conn, error := h.connectWithRetries(addr)
		if error != nil {
			continue
		}
		go h.sendAtIntervals(conn)
	}
	return nil
}

func (h *heartbeatHandler) StartReceiving(onTimeoutFunc func(amountOfHeartbeats int), timeoutAmount int) error {
	addr := fmt.Sprintf("%s:%d", h.host, h.port)
	conn, err := h.connectWithRetries(addr)
	if err != nil {
		return err
	}
	go h.recieveHeartbeats(conn, timeoutAmount, onTimeoutFunc)
	return nil
}

func (h *heartbeatHandler) ChangeAddress(host string, port int) {
	h.Close()
	h.host = host
	h.port = port
}

func (h *heartbeatHandler) Close() {
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
}

func (h *heartbeatHandler) Stop() {
	if h.cancel != nil {
		h.cancel()
		// Creates a new context for potential restart
		h.ctx, h.cancel = context.WithCancel(context.Background())
	}
}

// ------------ Private Methods ------------

func (h *heartbeatHandler) connectWithRetries(addr string) (*net.UDPConn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve UDP address: %w", err)
	}
	var conn *net.UDPConn
	for attempt := 1; attempt <= MAX_CONNECTION_RETRIES; attempt++ {
		conn, err = net.ListenUDP("udp", udpAddr)
		if err == nil {
			return conn, nil
		}
		time.Sleep(CONNECTION_RETRY_DELAY * time.Second)
	}
	return nil, fmt.Errorf("failed to connect to UDP address %s after %d attempts: %w",
		addr, MAX_CONNECTION_RETRIES, err)
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

func (h *heartbeatHandler) recieveHeartbeats(conn *net.UDPConn, timeoutAmount int, onTimeoutFunc func(amountOfHeartbeats int)) {
	buf := make([]byte, BUFFER_SIZE)

	heartbeatCounter := 0

	for {
		conn.SetReadDeadline(time.Now().Add(time.Duration(timeoutAmount) * time.Second))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil { // timeout or no data
			logger.Logger.Debug("Heartbeat Timeout Detected!")
			onTimeoutFunc(heartbeatCounter)
			conn.Close() // we stop receiving on timeout
			return
		}

		var hb protocol.HeartBeat
		if err := proto.Unmarshal(buf[:n], &hb); err != nil {
			logger.Logger.Warn("Failed to unmarshal heartbeat:", err)
			continue
		}
		heartbeatCounter++
	}
}
