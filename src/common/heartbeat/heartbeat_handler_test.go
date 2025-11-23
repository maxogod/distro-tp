package heartbeat_test

import (
	"os"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/heartbeat"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/stretchr/testify/assert"
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

// ----- Initialization Tests -----

func TestNewHeartBeatHandler(t *testing.T) {
	handler := heartbeat.NewHeartBeatHandler("localhost", 8080, 1)
	assert.NotNil(t, handler)

	// Clean up
	handler.Close()
}

func TestStartSending_InvalidAddress(t *testing.T) {
	handler := heartbeat.NewHeartBeatHandler("invalid-host", 8080, 1)
	defer handler.Close()

	err := handler.StartSending()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve UDP address")
}

func TestSendAndReceiveHeartbeats(t *testing.T) {
	receiverPort := 9090

	// Create receiver that will LISTEN on port 9090
	receiverHandler := heartbeat.NewHeartBeatHandler("localhost", receiverPort, 1)

	// Create sender that will SEND to port 9090
	senderHandler := heartbeat.NewHeartBeatHandler("localhost", receiverPort, 1)
	defer receiverHandler.Close()

	recieveChannel := make(chan int)

	// Start receiver FIRST (it needs to bind to the port)
	err := receiverHandler.StartReceiving(func(amountOfHeartbeats int) {
		recieveChannel <- amountOfHeartbeats
	}, 5)
	assert.NoError(t, err)

	// Give receiver time to start listening
	time.Sleep(100 * time.Millisecond)

	// Start sender AFTER receiver is listening
	err = senderHandler.StartSending()
	assert.NoError(t, err)

	// Allow enough time for at least 3 heartbeats (1 second interval + buffer)
	time.Sleep(4 * time.Second)

	// Simulate a timeout by closing the sender
	senderHandler.Close()

	// Check how many heartbeats were received before timeout
	recievedCount := <-recieveChannel
	assert.Greater(t, recievedCount, 0, "Should receive at least 1 heartbeat before timeout")
	t.Log("Amount of heartbeats received before timeout: ", recievedCount)
}

func TestSendMultipleHeartBeatsAndReceiveHeartbeats(t *testing.T) {
	receiverPort := 9090
	// Create receiver that will LISTEN on port 9090
	receiverHandler1 := heartbeat.NewHeartBeatHandler("localhost", receiverPort, 1)
	receiverHandler2 := heartbeat.NewHeartBeatHandler("localhost", receiverPort+1, 1)
	receiverHandler3 := heartbeat.NewHeartBeatHandler("localhost", receiverPort+2, 1)

	recieveHandlers := map[string]heartbeat.HeartBeatHandler{
		"handler1": receiverHandler1,
		"handler2": receiverHandler2,
		"handler3": receiverHandler3,
	}

	addrs := []string{
		"localhost:" + string(rune(receiverPort)),
		"localhost:" + string(rune(receiverPort+1)),
		"localhost:" + string(rune(receiverPort+2)),
	}

	// Create sender that will SEND to ports 9090, 9091, 9092
	senderHandler := heartbeat.NewReverseHeartBeatHandler(1)

	defer receiverHandler1.Close()
	defer receiverHandler2.Close()
	defer receiverHandler3.Close()

	handlerChan := make(chan string)

	for i, handler := range recieveHandlers {
		err := handler.StartReceiving(func(amountOfHeartbeats int) {
			// We ensure that each handler reports its own name upon timeout
			handlerChan <- i
		}, 5)
		assert.NoError(t, err)
	}

	// Give receiver time to start listening
	time.Sleep(100 * time.Millisecond)

	// Start sender AFTER receiver is listening
	err := senderHandler.StartSendingToAll(addrs, 3)
	assert.NoError(t, err)

	// Allow enough time for at least 3 heartbeats (1 second interval + buffer)
	time.Sleep(4 * time.Second)

	presentCounter := 0
	for i := range handlerChan {
		if _, exists := recieveHandlers[i]; exists {
			presentCounter++
		}
		if presentCounter >= len(recieveHandlers) {
			break
		}
	}

	senderHandler.Close()
}
