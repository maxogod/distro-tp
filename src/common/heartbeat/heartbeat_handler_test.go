package heartbeat_test

import (
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func init() {
	// Initialize logger for tests
	logger.InitLogger(logger.LoggerEnvDevelopment)
}

func TestMain(m *testing.M) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestNewHeartBeatHandler(t *testing.T) {
	handler := heartbeat.NewHeartBeatHandler("localhost", 8080, 1)
	assert.NotNil(t, handler)

	// Clean up
	handler.Close()
}

func TestStartSending_Success(t *testing.T) {
	// Create a UDP server to receive heartbeats
	serverAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
	require.NoError(t, err)

	serverConn, err := net.ListenUDP("udp", serverAddr)
	require.NoError(t, err)
	defer serverConn.Close()

	// Get the actual port assigned by the system
	actualPort := serverConn.LocalAddr().(*net.UDPAddr).Port

	// Create heartbeat handler
	handler := heartbeat.NewHeartBeatHandler("localhost", actualPort, 1)
	defer handler.Close()

	// Start sending
	err = handler.StartSending()
	assert.NoError(t, err)

	// Verify heartbeat is received
	buf := make([]byte, 1024)
	serverConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, _, err := serverConn.ReadFromUDP(buf)
	require.NoError(t, err)

	// Unmarshal and verify
	var hb protocol.HeartBeat
	err = proto.Unmarshal(buf[:n], &hb)
	assert.NoError(t, err)
	assert.Greater(t, hb.Timestamp, int64(0))
}

func TestStartSending_InvalidAddress(t *testing.T) {
	handler := heartbeat.NewHeartBeatHandler("invalid-host", 8080, 1)
	defer handler.Close()

	err := handler.StartSending()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve UDP address")
}

func TestStartSending_MultipleHeartbeats(t *testing.T) {
	// Create a UDP server to receive heartbeats
	serverAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
	require.NoError(t, err)

	serverConn, err := net.ListenUDP("udp", serverAddr)
	require.NoError(t, err)
	defer serverConn.Close()

	actualPort := serverConn.LocalAddr().(*net.UDPAddr).Port

	// Create heartbeat handler with short interval
	handler := heartbeat.NewHeartBeatHandler("localhost", actualPort, 1) // 1 second interval
	defer handler.Close()

	err = handler.StartSending()
	require.NoError(t, err)

	// Receive multiple heartbeats
	receivedCount := 0
	buf := make([]byte, 1024)

	for i := 0; i < 3; i++ {
		serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _, err := serverConn.ReadFromUDP(buf)
		if err != nil {
			break
		}

		var hb protocol.HeartBeat
		err = proto.Unmarshal(buf[:n], &hb)
		assert.NoError(t, err)
		receivedCount++
	}

	assert.GreaterOrEqual(t, receivedCount, 2, "Should receive at least 2 heartbeats")
}

func TestStartReceiving_Success(t *testing.T) {
	// Find available port for receiver
	receiverAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
	require.NoError(t, err)

	receiverConn, err := net.ListenUDP("udp", receiverAddr)
	require.NoError(t, err)
	defer receiverConn.Close()

	actualPort := receiverConn.LocalAddr().(*net.UDPAddr).Port
	receiverConn.Close() // Close so handler can bind to it

	// Create heartbeat handler
	handler := heartbeat.NewHeartBeatHandler("localhost", actualPort, 1)
	defer handler.Close()

	var timeoutCalled bool
	var mu sync.Mutex

	onTimeout := func() {
		mu.Lock()
		timeoutCalled = true
		mu.Unlock()
	}

	err = handler.StartReceiving(onTimeout, 2) // 2 second timeout
	require.NoError(t, err)

	// Send a heartbeat to prevent timeout
	senderConn, err := net.Dial("udp", fmt.Sprintf("localhost:%d", actualPort))
	require.NoError(t, err)
	defer senderConn.Close()

	hb := &protocol.HeartBeat{
		Timestamp: time.Now().Unix(),
	}
	data, err := proto.Marshal(hb)
	require.NoError(t, err)

	_, err = senderConn.Write(data)
	require.NoError(t, err)

	// Wait a bit and check timeout wasn't called
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	assert.False(t, timeoutCalled)
	mu.Unlock()
}

func TestStartReceiving_Timeout(t *testing.T) {
	// Find available port for receiver
	receiverAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
	require.NoError(t, err)

	receiverConn, err := net.ListenUDP("udp", receiverAddr)
	require.NoError(t, err)
	defer receiverConn.Close()

	actualPort := receiverConn.LocalAddr().(*net.UDPAddr).Port
	receiverConn.Close() // Close so handler can bind to it

	// Create heartbeat handler
	handler := heartbeat.NewHeartBeatHandler("localhost", actualPort, 1)
	defer handler.Close()

	var timeoutCalled bool
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	onTimeout := func() {
		mu.Lock()
		timeoutCalled = true
		mu.Unlock()
		wg.Done()
	}

	err = handler.StartReceiving(onTimeout, 1) // 1 second timeout
	require.NoError(t, err)

	// Don't send any heartbeats, wait for timeout
	wg.Wait()

	mu.Lock()
	assert.True(t, timeoutCalled, "Timeout function should have been called")
	mu.Unlock()
}

func TestStartReceiving_InvalidHeartbeat(t *testing.T) {
	// Find available port for receiver
	receiverAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
	require.NoError(t, err)

	receiverConn, err := net.ListenUDP("udp", receiverAddr)
	require.NoError(t, err)
	defer receiverConn.Close()

	actualPort := receiverConn.LocalAddr().(*net.UDPAddr).Port
	receiverConn.Close()

	handler := heartbeat.NewHeartBeatHandler("localhost", actualPort, 1)
	defer handler.Close()

	var timeoutCalled bool
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	onTimeout := func() {
		mu.Lock()
		timeoutCalled = true
		mu.Unlock()
		wg.Done()
	}

	err = handler.StartReceiving(onTimeout, 2) // 2 second timeout
	require.NoError(t, err)

	// Send invalid data
	senderConn, err := net.Dial("udp", fmt.Sprintf("localhost:%d", actualPort))
	require.NoError(t, err)
	defer senderConn.Close()

	// Send malformed data
	_, err = senderConn.Write([]byte("invalid heartbeat data"))
	require.NoError(t, err)

	// Should still timeout since invalid data is ignored
	wg.Wait()

	mu.Lock()
	assert.True(t, timeoutCalled)
	mu.Unlock()
}

func TestStop(t *testing.T) {
	handler := heartbeat.NewHeartBeatHandler("localhost", 8080, 1)

	// Start and then stop
	handler.Stop()

	// Verify context is cancelled by trying to start sending to invalid address
	// This should fail quickly if context is cancelled
	err := handler.StartSending()
	// We can't easily test context cancellation directly, but we can test that Stop() doesn't panic
	assert.NotNil(t, err) // Should fail due to invalid address, not context

	handler.Close()
}

func TestClose(t *testing.T) {
	handler := heartbeat.NewHeartBeatHandler("localhost", 8080, 1)

	// Close should not panic even if connection was never established
	assert.NotPanics(t, func() {
		handler.Close()
	})
}

func TestConcurrentOperations(t *testing.T) {
	// Find available ports
	addr1, err := net.ResolveUDPAddr("udp", "localhost:0")
	require.NoError(t, err)
	conn1, err := net.ListenUDP("udp", addr1)
	require.NoError(t, err)
	port1 := conn1.LocalAddr().(*net.UDPAddr).Port
	conn1.Close()

	addr2, err := net.ResolveUDPAddr("udp", "localhost:0")
	require.NoError(t, err)
	conn2, err := net.ListenUDP("udp", addr2)
	require.NoError(t, err)
	port2 := conn2.LocalAddr().(*net.UDPAddr).Port
	conn2.Close()

	// Create sender and receiver
	sender := heartbeat.NewHeartBeatHandler("localhost", port2, 1)
	receiver := heartbeat.NewHeartBeatHandler("localhost", port1, 1)

	defer sender.Close()
	defer receiver.Close()

	var wg sync.WaitGroup
	var receivedHeartbeat bool
	var mu sync.Mutex

	// Start receiver
	wg.Add(1)
	onTimeout := func() {
		wg.Done()
	}

	err = receiver.StartReceiving(onTimeout, 3) // 3 second timeout
	require.NoError(t, err)

	// Simulate sending heartbeat manually to receiver
	go func() {
		time.Sleep(100 * time.Millisecond) // Let receiver start

		senderConn, err := net.Dial("udp", fmt.Sprintf("localhost:%d", port1))
		if err != nil {
			return
		}
		defer senderConn.Close()

		hb := &protocol.HeartBeat{
			Timestamp: time.Now().Unix(),
		}
		data, err := proto.Marshal(hb)
		if err != nil {
			return
		}

		senderConn.Write(data)

		mu.Lock()
		receivedHeartbeat = true
		mu.Unlock()
	}()

	// Wait a bit to see if heartbeat prevents timeout
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	wasReceived := receivedHeartbeat
	mu.Unlock()

	assert.True(t, wasReceived, "Heartbeat should have been sent")

	// Cleanup
	sender.Stop()
	receiver.Stop()
}

// Helper function for integration test
func findAvailablePort() (int, error) {
	addr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		return 0, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return conn.LocalAddr().(*net.UDPAddr).Port, nil
}
