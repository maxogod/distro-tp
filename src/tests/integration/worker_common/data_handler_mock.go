package worker_common_test

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/worker"
	"google.golang.org/protobuf/proto"
)

type MockDataExecutor struct {
	ClientDataHandled        map[string][]*protocol.DataEnvelope
	HandleFinishClientCalled map[string]chan bool
}

type MockDataHandler struct {
	mockDataExecutor MockDataExecutor
}

func NewMockDataExecutor() MockDataExecutor {
	return MockDataExecutor{
		ClientDataHandled:        make(map[string][]*protocol.DataEnvelope),
		HandleFinishClientCalled: make(map[string]chan bool),
	}
}

func NewMockDataHandler(mockDataExecutor MockDataExecutor) worker.DataHandler {
	return &MockDataHandler{
		mockDataExecutor: mockDataExecutor,
	}
}

func (m *MockDataHandler) HandleData(dataEnvelope *protocol.DataEnvelope) error {

	if dataEnvelope == nil {
		return nil
	}

	clientID := dataEnvelope.ClientId
	m.mockDataExecutor.ClientDataHandled[clientID] = append(m.mockDataExecutor.ClientDataHandled[clientID], dataEnvelope)
	m.mockDataExecutor.HandleFinishClientCalled[clientID] = make(chan bool, 1)
	return nil
}

func (m *MockDataHandler) HandleFinishClient(clientID string) error {

	_, exists := m.mockDataExecutor.HandleFinishClientCalled[clientID]
	if !exists {
		m.mockDataExecutor.HandleFinishClientCalled[clientID] = make(chan bool, 1)
	}

	m.mockDataExecutor.HandleFinishClientCalled[clientID] <- true
	return nil
}

func (m *MockDataHandler) Close() error {
	return nil
}

func SendToQueue(queue middleware.MessageMiddleware, message *protocol.DataEnvelope) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	e := queue.Send(data)

	if e != middleware.MessageMiddlewareSuccess {
		return fmt.Errorf("Failed to send message to queue: %v", e)
	}
	return nil
}
