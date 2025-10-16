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
