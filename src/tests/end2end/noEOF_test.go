package eof_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/network"
	mock "github.com/maxogod/distro-tp/src/tests/end2end/io"
)

func testNoEof(t *testing.T) {
	t.Log("Starting No EOF test")
	// Connect to the server
	clientConnection := network.NewConnection()
	err := clientConnection.Connect(address, 5)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	dataBytes := getDataBytes(t, &mock.MockTransactionsBatchT1, enum.T1)
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
		t.Fatalf("Expected no response, but received data: %v", data)
	case err := <-errCh:
		t.Logf("Received error as expected: %v", err)
	case <-time.After(30 * time.Second):
		t.Log("Test passed: No data received within 30 seconds as expected")
	}

	t.Log("client never got any response as expected. Now checking logs...")

	out, err := runCommand("docker", "compose", "logs", "aggregator")
	if err != nil {
		t.Fatalf("Failed to get docker compose logs: %v", err)
	}

	// Regex to find the full line, e.g. "Client [123e4567-e89b-12d3-a456-426614174000] finished."
	re := regexp.MustCompile(`Client \[.*?\] finished\.`)
	match := re.FindString(out)

	if match != "" {
		t.Fatalf("Found unexpected 'finished' message in logs: %s\nThis suggests the client finished without EOF", match)
	}
	t.Log("Success! No client finish message found in logs (as expected when no EOF is sent)")
	clientConnection.Close()

}
