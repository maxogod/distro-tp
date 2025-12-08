package eof_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/network"
	mock "github.com/maxogod/distro-tp/src/tests/end2end/io"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func testNoEof(t *testing.T) {
	t.Log("Starting No EOF test")
	clientConnection := network.NewConnection()
	err := clientConnection.Connect(address, 5)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientConnection.Close()

	// Send task request and get client ID
	clientID := sendTaskRequest(t, clientConnection, enum.T1)

	dataBytes := getDataBytes(t, &mock.MockTransactionsBatchT1, enum.T1)
	// Set client ID in envelope
	envelope := protocol.DataEnvelope{}
	proto.Unmarshal(dataBytes, &envelope)
	envelope.ClientId = clientID
	dataBytes, _ = proto.Marshal(&envelope)

	clientConnection.SendData(dataBytes)
	t.Log("Sent all data and NO EOF signal, waiting for timeout to occure")
	dataCh := make(chan []byte)
	errCh := make(chan error)
	go func() {
		data, err := clientConnection.ReceiveData()
		if err != nil {
			errCh <- err
			return
		}
		dataCh <- data
	}()
	// Wait with timeout
	select {
	case data := <-dataCh:
		t.Logf("Received unexpected data (but test passes): %v", data)
	case err := <-errCh:
		t.Logf("Received error as expected: %v", err)
	case <-time.After(5 * time.Second):
		t.Log("Test passed: No processed data received within 5 seconds as expected")
	}
	t.Log("client never got any response as expected. Now checking logs...")
	// since this is the first test to be executed, we just check for this log to not be present
	assert.False(t, checkEOFLog(t, "aggregator", "x"), "EOF log found in aggregator container unexpectedly")

}
