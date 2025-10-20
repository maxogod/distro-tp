package eof_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/network"
	mock "github.com/maxogod/distro-tp/src/tests/end2end/io"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

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
	storeRefDataBytes := getStoreDataBytes(t)
	clientConnection.SendData(storeRefDataBytes)

	doneRef := getEOFDataBytes(t, enum.T3, true)
	clientConnection.SendData(doneRef)

	// --- Send transactions batch ---
	dataBytes := getDataBytes(t, &mock.MockTransactionsBatchT3, enum.T3)
	clientConnection.SendData(dataBytes)

	doneData := getEOFDataBytes(t, enum.T3, false)
	clientConnection.SendData(doneData)

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

	assert.Equal(t, len(mock.MockTPVOutput), len(output), "Expected the same output length")
	for i, tx := range output {
		expectedTx := mock.MockTPVOutput[i]
		assert.Equal(t, expectedTx.StoreId, tx.StoreId, "Store ID should match")
		assert.Equal(t, expectedTx.Semester, tx.Semester, "Semester should match")
		assert.Equal(t, expectedTx.FinalAmount, tx.FinalAmount, "Final Amount should match")
	}

	t.Log("client outputs verified successfully. Now checking logs...")
	time.Sleep(1 * time.Minute)

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
