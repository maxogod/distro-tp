package eof_test

import (
	"bytes"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/network"
	eof "github.com/maxogod/distro-tp/src/tests/end2end"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var address = "localhost:8080"
var dockerComposeFile = "docker-compose-test.yaml"
var healthCheckAddress = "localhost:8081/ping"

func TestSequentialRun(t *testing.T) {

	tests := []func(t *testing.T){
		testOnetoOneEof,
		// testOnetoN,
		// testOnetoOneNoEOF,
		// testNtoM,
	}

	// Run each test one by one
	for _, test := range tests {
		t.Log("Setting up enviroment")
		_, err := runCommand("docker", "compose", "-f", dockerComposeFile, "up", "-d")
		if err != nil {
			t.Fatalf("Failed to start docker compose: %v", err)
		}
		runHealthCheck(healthCheckAddress)
		test(t)
		t.Log("Restarting environment for next test...")
		runCommand("docker", "compose", "stop")
		t.Log("================================================")
		time.Sleep(1 * time.Second)
	}
	//runCommand("docker", "compose", "down")
	t.Log("All tests completed!")
}

func testOnetoN(t *testing.T) {
	t.Log("Starting One-to-N test")
	// Connect to the server
	clientConnection := network.NewConnection()
	err := clientConnection.Connect(address, 5)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientConnection.Close()

	// ============================================================

	// --- Send store reference data ---
	sendStoreRefData(t, clientConnection, true)

	// --- Send transactions batch ---
	sendData(t, clientConnection, &eof.MockTransactionsBatchT3, enum.T3, true)

	// ============================================================

	// --- Receive output ---

	t.Log("Sent all data and EOF signal, waiting for output...")
	time.Sleep(100 * time.Millisecond)
	output := map[string]*reduced.TotalPaymentValue{}
	for {
		receivedData, err := clientConnection.ReceiveData()
		if err != nil {
			t.Fatalf("Failed to receive data: %v", err)
		}
		receivedEnvelope := protocol.DataEnvelope{}
		err = proto.Unmarshal(receivedData, &receivedEnvelope)
		if err != nil {
			t.Fatalf("Failed to deserialize received data: %v", err)
		}
		if receivedEnvelope.IsDone {
			break // End of data
		}
		tpv := reduced.TotalPaymentValue{}
		err = proto.Unmarshal(receivedEnvelope.Payload, &tpv)
		if err != nil {
			t.Fatalf("Failed to deserialize transaction batch: %v", err)
		}
		output[tpv.StoreId] = &tpv
	}

	// ============================================================

	// Review Outputs and logs

	assert.Equal(t, len(eof.MockTPVOutput), len(output), "Expected the same output length")
	for i, tx := range output {
		expectedTx := eof.MockTPVOutput[i]
		assert.Equal(t, expectedTx.StoreId, tx.StoreId, "Store ID should match")
		assert.Equal(t, expectedTx.Semester, tx.Semester, "Semester should match")
		assert.Equal(t, expectedTx.FinalAmount, tx.FinalAmount, "Final Amount should match")
	}

	// After test, we check the logs

	// Compile the regex once
	re := regexp.MustCompile(`Client \[.*?\] finished\.`)

	// Containers to check
	containers := []string{"aggregator", "joiner1", "joiner2"}

	var match string
	for _, container := range containers {
		if match = checkLogForPattern(t, container, re); match != "" {
			t.Logf("Success! Found log: [ %s ] in %s", match, container)
		} else {
			t.Fatalf("Expected log not found in container %s: pattern 'Client [<id>] finished.'", container)
		}
	}

	t.Log("Test passed Successfully!")

}
func testOnetoOneNoEOF(t *testing.T) {
	t.Log("Starting One-to-One No EOF test")

	// Connect to the server
	clientConnection := network.NewConnection()
	err := clientConnection.Connect(address, 5)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientConnection.Close()

	// We begin to send data for task type 1
	// This demonstrates a one-to-one communication because
	// there is only one client and one node that handles the EOF (aggregator)

	sendData(t, clientConnection, &eof.MockTransactionsBatchT1, enum.T1, false)
	t.Log("Sent transaction batch for client1. with NO EOF")

	done := make(chan bool, 1)

	go func() {
		// Attempt to receive any data
		receivedData, err := clientConnection.ReceiveData()
		if err == nil && len(receivedData) > 0 {
			t.Errorf("Unexpected data received: %v", receivedData)
			done <- false
			return
		}
		done <- true
	}()

	select {
	case ok := <-done:
		if !ok {
			t.FailNow()
		}
	case <-time.After(1 * time.Minute):
		t.Log("✅ No data received within 1 minute (expected behavior, no EOF sent).")
	}

	t.Log("Sent all data and NO EOF signal")

}

func testNtoM(t *testing.T) {
	t.Log("Starting N-to-M concurrent test with output collection")

	numClients := 3
	clients := make([]network.ConnectionInterface, numClients)
	defer func() {
		for _, c := range clients {
			if c != nil {
				c.Close()
			}
		}
	}()

	// --- Connect all clients ---
	for i := 0; i < numClients; i++ {
		client := network.NewConnection()
		if err := client.Connect(address, 5); err != nil {
			t.Fatalf("Client %d failed to connect: %v", i+1, err)
		}
		clients[i] = client
	}
	t.Logf("All %d clients connected successfully.", numClients)

	// --- Channel to collect outputs ---
	outputCh := make(chan map[string]*reduced.TotalPaymentValue, numClients)

	// --- Run clients secuentially ---
	for i, client := range clients {

		go func(i int, client network.ConnectionInterface) {

			t.Logf("Client %d sending store reference data", i+1)
			sendStoreRefData(t, client, true)

			t.Logf("Client %d sending transaction batch", i+1)
			sendData(t, client, &eof.MockTransactionsBatchT3, enum.T3, true)

			t.Logf("Client %d waiting for output", i+1)
			output := map[string]*reduced.TotalPaymentValue{}
			for {
				data, err := client.ReceiveData()
				if err != nil {
					t.Fatalf("Client %d failed to receive data: %v", i+1, err)
				}
				envelope := protocol.DataEnvelope{}
				if err := proto.Unmarshal(data, &envelope); err != nil {
					t.Fatalf("Client %d failed to unmarshal envelope: %v", i+1, err)
				}
				if envelope.IsDone {
					break
				}
				tpv := reduced.TotalPaymentValue{}
				if err := proto.Unmarshal(envelope.Payload, &tpv); err != nil {
					t.Fatalf("Client %d failed to unmarshal TPV: %v", i+1, err)
				}
				output[tpv.StoreId] = &tpv
			}
			// Send output to channel
			outputCh <- output
		}(i, client)
	}

	// --- Collect outputs ---
	allOutputs := make([]map[string]*reduced.TotalPaymentValue, 0, numClients)
	for out := range outputCh {
		allOutputs = append(allOutputs, out)
	}

	t.Log("All clients finished and output collected.")

	// --- Optional: Validate outputs ---
	for i, output := range allOutputs {
		assert.Equal(t, len(eof.MockTPVOutput), len(output), "Client %d: expected output length", i+1)
		for j, tx := range output {
			expectedTx := eof.MockTPVOutput[j]
			assert.Equal(t, expectedTx.StoreId, tx.StoreId, "Client %d: Store ID", i+1)
			assert.Equal(t, expectedTx.Semester, tx.Semester, "Client %d: Semester", i+1)
			assert.Equal(t, expectedTx.FinalAmount, tx.FinalAmount, "Client %d: Final Amount", i+1)
		}
	}

	// --- Check logs after all clients finished ---
	re := regexp.MustCompile(`Client \[.*?\] finished\.`)
	containers := []string{"aggregator", "joiner1", "joiner2"}
	totalMatches := 0

	for _, container := range containers {
		matchCount := countLogMatches(t, container, re)
		t.Logf("Container %s had %d matches", container, matchCount)
		totalMatches += matchCount
	}

	assert.Equal(t, numClients, totalMatches, "Expected %d clients to appear finished in logs", numClients)
	t.Log("N-to-M concurrent test passed successfully!")

}

// -------- Helper functions for testing --------

func runCommand(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

func checkLogForPattern(t *testing.T, container string, pattern *regexp.Regexp) string {
	out, err := runCommand("docker", "compose", "logs", container)
	if err != nil {
		t.Fatalf("Failed to get docker compose logs for %s: %v", container, err)
	}
	return pattern.FindString(out)
}

func runHealthCheck(address string) error {
	const maxAttempts = 10
	const wait = 1 * time.Second
	for range maxAttempts {
		resp, err := http.Get(address)
		if err == nil && resp.StatusCode == http.StatusOK {
			return nil
		}
		time.Sleep(wait)
	}
	return fmt.Errorf("health check failed for %s after %d attempts", address, maxAttempts)
}

func countLogMatches(t *testing.T, container string, pattern *regexp.Regexp) int {
	out, err := runCommand("docker", "compose", "logs", container)
	if err != nil {
		t.Fatalf("Failed to get docker compose logs for %s: %v", container, err)
	}
	return len(pattern.FindAllString(out, -1))
}

func sendStoreRefData(t *testing.T, client network.ConnectionInterface, sendEof bool) {

	serializedStoreBatch, err := proto.Marshal(&eof.MockStoreRefData)
	if err != nil {
		t.Fatalf("Client failed to serialize store reference data: %v", err)
	}

	sockReferenceEnvelope := protocol.ReferenceEnvelope{
		Payload:       serializedStoreBatch,
		ReferenceType: int32(enum.Stores),
	}

	serializedRefEnvelope, err := proto.Marshal(&sockReferenceEnvelope)
	if err != nil {
		t.Fatalf("Client failed to serialize reference envelope: %v", err)
	}

	storeDataEnvelope := protocol.DataEnvelope{
		IsRef:    true,
		TaskType: int32(enum.T3),
		Payload:  serializedRefEnvelope,
	}

	serializedStoreEnvelope, err := proto.Marshal(&storeDataEnvelope)
	if err != nil {
		t.Fatalf("Client failed to serialize store data envelope: %v", err)
	}

	err = client.SendData(serializedStoreEnvelope)
	if err != nil {
		t.Fatalf("Client failed to send store reference data: %v", err)
	}

	if !sendEof {
		return
	}

	// Send EOF for reference data
	refEofEnvelope := protocol.DataEnvelope{
		TaskType: int32(enum.T3),
		IsDone:   true,
		IsRef:    true,
	}
	serializedRefEofEnvelope, err := proto.Marshal(&refEofEnvelope)
	if err != nil {
		t.Fatalf("Client failed to serialize EOF envelope: %v", err)
	}
	err = client.SendData(serializedRefEofEnvelope)
	if err != nil {
		t.Fatalf("Client failed to send EOF signal: %v", err)
	}
}

func sendData(t *testing.T, client network.ConnectionInterface, dataBatch *raw.TransactionBatch, taskType enum.TaskType, sendEof bool) {
	serializedBatch, err := proto.Marshal(dataBatch)
	if err != nil {
		t.Fatalf("Failed to serialize transaction: %v", err)
	}
	dataEnvelope := protocol.DataEnvelope{
		TaskType: int32(taskType),
		Payload:  serializedBatch,
	}
	serializedEnvelope, err := proto.Marshal(&dataEnvelope)
	err = client.SendData(serializedEnvelope)
	if err != nil {
		t.Fatalf("Failed to send transaction: %v", err)
	}
	t.Log("Sent transaction batch for client1.")

	if !sendEof {
		return
	}

	// Send EOF signal
	eofEnvelope := protocol.DataEnvelope{
		TaskType: int32(taskType),
		IsDone:   true,
	}
	t.Log("Sending EOF signal for client")
	serializedEofEnvelope, err := proto.Marshal(&eofEnvelope)
	if err != nil {
		t.Fatalf("Failed to serialize EOF envelope: %v", err)
	}
	err = client.SendData(serializedEofEnvelope)
	if err != nil {
		t.Fatalf("Failed to send EOF signal: %v", err)
	}
}
