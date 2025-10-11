package worker_common_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/worker"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var url = "amqp://guest:guest@localhost:5672/"

type MockDataHandler struct {
	HandleDataCalled         bool
	HandleFinishClientCalled bool
}

func (m *MockDataHandler) HandleData(dataEnvelope *protocol.DataEnvelope) error {

	m.HandleDataCalled = true
	return nil
}

func (m *MockDataHandler) HandleFinishClient(clientID string) error {
	m.HandleFinishClientCalled = true
	return nil
}

func (m *MockDataHandler) Close() error {
	return nil
}

func TestNewMessageHandler(t *testing.T) {
	mockDataHandler := &MockDataHandler{}
	inputMiddleware, _ := middleware.NewQueueMiddleware(url, "inputQueue")
	defer inputMiddleware.Delete()

	inputQueues := []middleware.MessageMiddleware{inputMiddleware}

	mh := worker.NewMessageHandler(mockDataHandler, inputQueues, nil)
	if mh == nil {
		t.Errorf("Expected MessageHandler to be created, got nil")
	}
}

func TestMessageHandlerStart(t *testing.T) {
	mockDataHandler := &MockDataHandler{}
	inputMiddleware, _ := middleware.NewQueueMiddleware(url, "inputQueue")
	defer inputMiddleware.Delete()

	sendMessage := &protocol.DataEnvelope{
		TaskType: int32(1),
		Payload:  []byte("test payload"),
	}
	data, _ := proto.Marshal(sendMessage)
	inputMiddleware.Send(data)

	inputQueues := []middleware.MessageMiddleware{inputMiddleware}

	mh := worker.NewMessageHandler(mockDataHandler, inputQueues, nil)
	if mh == nil {
		t.Errorf("Expected MessageHandler to be created, got nil")
	}

	go func() {
		err := mh.Start()
		if err != nil {
			t.Errorf("MessageHandler.Start() returned an error: %v", err)
		}
	}()

	// Wait for the message to be processed
	time.Sleep(1 * time.Second)

	// Clean up
	assert.NoError(t, mh.Close())
	// Check if HandleData was called
	assert.True(t, mockDataHandler.HandleDataCalled, "Expected HandleData to be called")
}

func TestMessageHandlerWithFinish(t *testing.T) {
	mockDataHandler := &MockDataHandler{}

	inputMiddleware, _ := middleware.NewQueueMiddleware(url, "inputQueue")
	finisherQueue, _ := middleware.NewQueueMiddleware(url, "finisherQueue")
	defer inputMiddleware.Delete()
	defer finisherQueue.Delete()

	sendMessage := &protocol.DataEnvelope{
		ClientId: "client1",
		TaskType: int32(1),
		Payload:  []byte("test payload"),
	}
	data, _ := proto.Marshal(sendMessage)
	inputMiddleware.Send(data)

	inputQueues := []middleware.MessageMiddleware{inputMiddleware}

	mh := worker.NewMessageHandler(mockDataHandler, inputQueues, finisherQueue)

	go func() {
		err := mh.Start()
		if err != nil {
			t.Errorf("MessageHandler.Start() returned an error: %v", err)
		}
	}()

	// Wait for the message to be processed
	time.Sleep(1 * time.Second)

	// Check if HandleData was called
	assert.True(t, mockDataHandler.HandleDataCalled, "Expected HandleData to be called")

	sendFinishMessage := &protocol.DataEnvelope{
		ClientId: "client1",
	}
	finishData, _ := proto.Marshal(sendFinishMessage)
	finisherQueue.Send(finishData)

	// Wait for the finish message to be processed
	for !mockDataHandler.HandleFinishClientCalled {
		time.Sleep(1 * time.Second)
	}
	assert.True(t, mockDataHandler.HandleFinishClientCalled, "Expected HandleFinishClient to be eventually called")
	assert.NoError(t, mh.Close())
}

func TestMessageHandlerWithMultipleInputQueues(t *testing.T) {
	mockDataHandler := &MockDataHandler{}
	inputMiddleware1, _ := middleware.NewQueueMiddleware(url, "inputQueue1")
	inputMiddleware2, _ := middleware.NewQueueMiddleware(url, "inputQueue2")
	defer inputMiddleware1.Delete()
	defer inputMiddleware2.Delete()

	sendMessage := &protocol.DataEnvelope{
		TaskType: int32(1),
		Payload:  []byte("test payload"),
	}
	data, _ := proto.Marshal(sendMessage)
	inputMiddleware1.Send(data)

	inputQueues := []middleware.MessageMiddleware{inputMiddleware1, inputMiddleware2}

	mh := worker.NewMessageHandler(mockDataHandler, inputQueues, nil)
	if mh == nil {
		t.Errorf("Expected MessageHandler to be created, got nil")
	}

	go func() {
		err := mh.Start()
		if err != nil {
			t.Errorf("MessageHandler.Start() returned an error: %v", err)
		}
	}()

	// Wait for the message to be processed
	time.Sleep(1 * time.Second)

	assert.True(t, mockDataHandler.HandleDataCalled, "Expected HandleData to be called")
	mockDataHandler.HandleDataCalled = false
	assert.False(t, mockDataHandler.HandleDataCalled, "Expected HandleData to be reset")

	// Send message to the second input queue
	inputMiddleware2.Send(data)

	// Wait for the message to be processed
	time.Sleep(1 * time.Second)
	assert.True(t, mockDataHandler.HandleDataCalled, "Expected HandleData to be called for second input queue")
}
