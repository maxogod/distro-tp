package heartbeat_test

import (
	"fmt"
	"sync/atomic"
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

func TestNewHeartBeatHandler(t *testing.T) {
	handler, err := heartbeat.NewListeningHeartBeatHandler("localhost", 8080, 1*time.Second)
	assert.NoError(t, err)
	assert.NotNil(t, handler)

	// Clean up
	handler.Close()
}

func TestStartSending_InvalidAddress(t *testing.T) {
	handler, err := heartbeat.NewListeningHeartBeatHandler("invalid-host", 8080, 1*time.Second)
	assert.Error(t, err)
	assert.Nil(t, handler)
}

func TestSendAndReceiveHeartbeats(t *testing.T) {
	receiverPort := 9090

	// Create receiver that will LISTEN on port 9090
	receiverHandler, err := heartbeat.NewListeningHeartBeatHandler("localhost", receiverPort, 1*time.Second)
	assert.NoError(t, err)

	// Create sender that will SEND to port 9090
	senderHandler := heartbeat.NewHeartBeatHandler("localhost", receiverPort, 1)
	defer receiverHandler.Close()

	recieveChannel := make(chan int)

	// Start receiver with a 1 second timeout
	err = receiverHandler.StartReceiving(func(amountOfHeartbeats int) {
		recieveChannel <- amountOfHeartbeats
	}, 1)
	assert.NoError(t, err)

	// Start Sender
	err = senderHandler.StartSending()
	assert.NoError(t, err)

	// Simulate a timeout by closing the Sender
	time.Sleep(100 * time.Millisecond)
	senderHandler.Close()

	// Check how many heartbeats were received before timeout
	recievedCount := <-recieveChannel
	assert.Greater(t, recievedCount, 0, "Should receive at least 1 heartbeat before timeout")
	t.Log("Amount of heartbeats received before timeout: ", recievedCount)
}

func TestSendMultipleHeartBeatsAndReceiveHeartbeats(t *testing.T) {
	receiverPort := 9090
	// Create receiver that will LISTEN on port 9090
	receiverHandler1, err := heartbeat.NewListeningHeartBeatHandler("localhost", receiverPort, 1*time.Second)
	assert.NoError(t, err)
	receiverHandler2, err := heartbeat.NewListeningHeartBeatHandler("localhost", receiverPort+1, 1*time.Second)
	assert.NoError(t, err)
	receiverHandler3, err := heartbeat.NewListeningHeartBeatHandler("localhost", receiverPort+2, 1*time.Second)
	assert.NoError(t, err)

	recieveHandlers := []heartbeat.HeartBeatHandler{
		receiverHandler1,
		receiverHandler2,
		receiverHandler3,
	}

	addrs := []string{
		fmt.Sprintf("localhost:%d", receiverPort),
		fmt.Sprintf("localhost:%d", receiverPort+1),
		fmt.Sprintf("localhost:%d", receiverPort+2),
	}

	// Create sender that will SEND to ports 9090, 9091, 9092
	senderHandler := heartbeat.NewHeartBeatHandler("localhost", 8080, 1)

	defer receiverHandler1.Close()
	defer receiverHandler2.Close()
	defer receiverHandler3.Close()
	defer senderHandler.Close()

	var handlerCounter atomic.Int64

	for i, handler := range recieveHandlers {
		err := handler.StartReceiving(func(amountOfHeartbeats int) {
			// if a timeout occurs, record which handler timed out
			t.Logf("Handler [%d] timed out", i)
			handlerCounter.Add(1)
		}, 1)
		assert.NoError(t, err)
	}

	// Start Sender
	err = senderHandler.StartSendingToAll(addrs)
	assert.NoError(t, err)

	// Stop sending after some time to allow receivers to timeout
	time.Sleep(100 * time.Second)
	senderHandler.Stop()

	for {
		if handlerCounter.Load() >= int64(len(recieveHandlers)) {
			break
		}
	}
	assert.Equal(t, len(recieveHandlers), int(handlerCounter.Load()), "All handlers should receive heartbeats before timeout")
}

func TestStopAndSwapSendReadActions(t *testing.T) {
	receiverPort := 9095

	// This handler will begin as a receiver
	handler1, err := heartbeat.NewListeningHeartBeatHandler("localhost", receiverPort, 1*time.Second)
	assert.NoError(t, err)
	defer handler1.Close()

	// This handler will begin as a sender
	handler2, err := heartbeat.NewListeningHeartBeatHandler("localhost", receiverPort+1, 1*time.Second)
	assert.NoError(t, err)
	defer handler2.Close()

	handlerChan := make(chan int)

	// Start receiver with a 2 second timeout
	err = handler1.StartReceiving(func(amountOfHeartbeats int) {
		t.Logf("Receiver timed out after %d heartbeats", amountOfHeartbeats)
		handlerChan <- 1
	}, 2)
	assert.NoError(t, err)

	// Start sending
	t.Log("Phase 1: Starting sender...")
	err = handler2.StartSendingToAll([]string{fmt.Sprintf("localhost:%d", receiverPort)})
	assert.NoError(t, err)

	// Send a few heartbeats then stop sending heartbeats to allow timeout
	t.Log("Phase 1: Stopping sender to allow timeout...")
	time.Sleep(1500 * time.Millisecond)
	handler2.Stop()

	// Wait for receiver to get heartbeats
	<-handlerChan
	t.Log("Phase 1: Heartbeats received by receiver.")

	t.Log("Phase 2: Swapping roles...")

	// Start handler2 as receiver with a 2 second timeout
	err = handler2.StartReceiving(func(amountOfHeartbeats int) {
		t.Logf("New Receiver timed out after %d heartbeats", amountOfHeartbeats)
		handlerChan <- 1
	}, 2)
	assert.NoError(t, err)

	// Start handler1 as sender
	err = handler1.StartSendingToAll([]string{fmt.Sprintf("localhost:%d", receiverPort+1)})
	assert.NoError(t, err)

	// Send a few heartbeats then stop sending heartbeats to allow timeout
	time.Sleep(1500 * time.Millisecond)
	handler1.Stop()

	// Wait for new receiver to get heartbeats
	<-handlerChan
	t.Log("Phase 2: Heartbeats received by new receiver.")

	t.Log("Test completed successfully.")

}

func TestStartRecieverWithNoSender(t *testing.T) {
	receiverPort := 9100
	interval := 1 * time.Second

	// Create receiver that will LISTEN on port 9100
	receiverHandler, err := heartbeat.NewListeningHeartBeatHandler("localhost", receiverPort, interval)
	assert.NoError(t, err)
	defer receiverHandler.Close()

	// this is to ensure that the StartReceiving can be called multiple times
	for i := range 3 {
		t.Logf("Attempt [%d] to recieve data ", i+1)

		recieveChannel := make(chan int)

		// Start receiver with a 1 second timeout
		err := receiverHandler.StartReceiving(func(amountOfHeartbeats int) {
			recieveChannel <- amountOfHeartbeats
		}, 1)
		assert.NoError(t, err)

		// Wait for timeout to occur
		recievedCount := <-recieveChannel
		assert.Equal(t, 0, recievedCount, "Should receive 0 heartbeats before timeout")
		t.Log("Amount of heartbeats received before timeout: ", recievedCount)
	}
}
