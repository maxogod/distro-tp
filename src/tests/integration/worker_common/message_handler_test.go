package worker_common_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/stretchr/testify/assert"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestSingleClientHandle(t *testing.T) {
	t.Log("Running testSingleClientHandle")

	mockDataExecutor := NewMockDataExecutor()
	mockDataHandler := NewMockDataHandler(mockDataExecutor)
	inputMiddleware1, _ := middleware.NewQueueMiddleware(url, "inputQueue1")
	finishExchange := middleware.GetFinishExchange(url, []string{"testWorker"})

	messageHandler := worker.NewMessageHandler(mockDataHandler, []middleware.MessageMiddleware{inputMiddleware1}, finishExchange)
	if messageHandler == nil {
		t.Errorf("Expected MessageHandler to be created, got nil")
	}

	go func() {

		e := messageHandler.Start()
		if e != nil {
			t.Log("Error starting message handler")
			return
		}
	}()

	sendMessage := &protocol.DataEnvelope{
		ClientId: "client1",
		TaskType: int32(1),
		Payload:  []byte("test payload"),
	}
	SendToQueue(inputMiddleware1, sendMessage)

	// Wait for the message to be processed
	time.Sleep(100 * time.Millisecond)

	// Check if HandleData was called
	handledClient, exists := mockDataExecutor.ClientDataHandled["client1"]

	assert.True(t, exists, "Expected HandleData to be called for client1")

	assert.Equal(t, 1, len(handledClient), "Expected HandleData to be called once for client1")
	assert.Equal(t, "test payload", string(handledClient[0].Payload), "Expected payload to match")
	assert.Equal(t, "client1", handledClient[0].ClientId, "Expected ClientId to match")
	assert.Equal(t, int32(1), handledClient[0].TaskType, "Expected TaskType to match")

	// Finish up the client
	sendDone := &protocol.DataEnvelope{
		ClientId: "client1",
		IsDone:   true,
	}
	SendToQueue(finishExchange, sendDone)

	finishedClient, exists := mockDataExecutor.HandleFinishClientCalled["client1"]

	t.Log("Waiting for HandleFinishClient to be called for client1")
	hasFinished := <-finishedClient

	assert.True(t, hasFinished, "Expected HandleFinishClient to be true for client1")

	// Close the message handler
	inputMiddleware1.Delete()
	finishExchange.Delete()
	messageHandler.Close()
}

func startMessageHandler(url string) (MockDataExecutor, worker.MessageHandler) {
	mockDataExecutor := NewMockDataExecutor()
	mockDataHandler := NewMockDataHandler(mockDataExecutor)
	inputMiddleware1, _ := middleware.NewQueueMiddleware(url, "inputQueue1")
	finishExchange := middleware.GetFinishExchange(url, []string{"testWorker"})

	messageHandler := worker.NewMessageHandler(mockDataHandler, []middleware.MessageMiddleware{inputMiddleware1}, finishExchange)
	if messageHandler == nil {
		panic("Expected MessageHandler to be created, got nil")
	}

	go func() {
		e := messageHandler.Start()
		if e != nil {
			return
		}
	}()
	return mockDataExecutor, messageHandler
}

func Test1ToManyHandle(t *testing.T) {
	t.Log("Running test1ToManyHandle")

	// For this test, we will start 3 workers to simulate a 1 to many scenario
	// We
	worker1, mh1 := startMessageHandler(url)
	worker2, mh2 := startMessageHandler(url)
	worker3, mh3 := startMessageHandler(url)

	inputMiddleware1, _ := middleware.NewQueueMiddleware(url, "inputQueue1")
	finishExchange := middleware.GetFinishExchange(url, []string{"testWorker"})

	// Since we have 3 workers, we expect each one to handle at least one message
	// But,we can't estimate how many messages each one will handle
	// So we will send 9 messages and expect each worker to handle at least 1 message

	clientID := "client1"

	for i := 0; i < 9; i++ {
		sendMessage := &protocol.DataEnvelope{
			ClientId: clientID,
			TaskType: int32(1),
			Payload:  []byte("test payload"),
		}
		SendToQueue(inputMiddleware1, sendMessage)
	}

	// Wait for the message to be processed
	time.Sleep(500 * time.Millisecond)

	// Check if HandleData was called
	handledClientInWorker1, exists := worker1.ClientDataHandled["client1"]
	assert.True(t, exists, "Expected HandleData to be called for client1 in worker1")

	handledClientInWorker2, exists := worker2.ClientDataHandled["client1"]
	assert.True(t, exists, "Expected HandleData to be called for client1 in worker2")

	handledClientInWorker3, exists := worker3.ClientDataHandled["client1"]
	assert.True(t, exists, "Expected HandleData to be called for client1 in worker3")

	totalHandled := len(handledClientInWorker1) + len(handledClientInWorker2) + len(handledClientInWorker3)
	assert.Equal(t, 9, totalHandled, "Expected total handled messages to be 9")

	// Check the details of the handled messages
	for _, handled := range handledClientInWorker1 {
		assert.Equal(t, "test payload", string(handled.Payload), "Expected payload to match in worker1")
		assert.Equal(t, clientID, handled.ClientId, "Expected ClientId to match in worker1")
		assert.Equal(t, int32(1), handled.TaskType, "Expected TaskType to match in worker1")
	}

	for _, handled := range handledClientInWorker2 {
		assert.Equal(t, "test payload", string(handled.Payload), "Expected payload to match in worker2")
		assert.Equal(t, clientID, handled.ClientId, "Expected ClientId to match in worker2")
		assert.Equal(t, int32(1), handled.TaskType, "Expected TaskType to match in worker2")
	}

	for _, handled := range handledClientInWorker3 {
		assert.Equal(t, "test payload", string(handled.Payload), "Expected payload to match in worker3")
		assert.Equal(t, clientID, handled.ClientId, "Expected ClientId to match in worker3")
		assert.Equal(t, int32(1), handled.TaskType, "Expected TaskType to match in worker3")
	}

	// Finish up the client for each worker
	sendDone := &protocol.DataEnvelope{
		ClientId: "client1",
		IsDone:   true,
	}
	SendToQueue(finishExchange, sendDone)

	finishedClientInWorker1, exists := worker1.HandleFinishClientCalled["client1"]
	t.Log("Waiting for HandleFinishClient to be called for worker1")
	hasFinished := <-finishedClientInWorker1
	assert.True(t, hasFinished, "Expected HandleFinishClient to be true for client1")

	finishedClientInWorker2, exists := worker2.HandleFinishClientCalled["client1"]
	t.Log("Waiting for HandleFinishClient to be called for worker2")
	hasFinished = <-finishedClientInWorker2
	assert.True(t, hasFinished, "Expected HandleFinishClient to be true for client1")

	finishedClientInWorker3, exists := worker3.HandleFinishClientCalled["client1"]
	t.Log("Waiting for HandleFinishClient to be called for worker3")
	hasFinished = <-finishedClientInWorker3
	assert.True(t, hasFinished, "Expected HandleFinishClient to be true for client1")

	// Close the message handler
	inputMiddleware1.Delete()
	finishExchange.Delete()
	mh1.Close()
	mh2.Close()
	mh3.Close()
}
