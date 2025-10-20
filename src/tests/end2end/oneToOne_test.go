package eof_test

import (
	"regexp"
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/network"
	mock "github.com/maxogod/distro-tp/src/tests/end2end/io"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func testOnetoOneEof(t *testing.T) {
	t.Log("Starting One-to-One EOF test")
	// Connect to the server
	clientConnection := network.NewConnection()
	err := clientConnection.Connect(address, 5)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// We begin to send data for task type 1
	// This demonstrates a one-to-one communication because
	// there is only one client and one node that handles the EOF (aggregator)
	dataBytes := getDataBytes(t, &mock.MockTransactionsBatchT1, enum.T1)
	doneData := getEOFDataBytes(t, enum.T1, false)
	clientConnection.SendData(dataBytes)
	clientConnection.SendData(doneData)

	t.Log("Sent all data and EOF signal, waiting for output...")

	output := []*raw.Transaction{}
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
		transactionBatch := raw.TransactionBatch{}
		err = proto.Unmarshal(receivedEnvelope.Payload, &transactionBatch)
		if err != nil {
			t.Fatalf("Failed to deserialize transaction batch: %v", err)
		}
		output = append(output, transactionBatch.Transactions...)
	}

	assert.Equal(t, len(mock.MockTransactionsOutput), len(output), "Expected the same output length")
	for i, tx := range output {
		expectedTx := mock.MockTransactionsOutput[i]
		assert.Equal(t, expectedTx.TransactionId, tx.TransactionId, "Transaction ID should match")
		assert.Equal(t, expectedTx.StoreId, tx.StoreId, "Store ID should match")
		assert.Equal(t, expectedTx.UserId, tx.UserId, "User ID should match")
		assert.Equal(t, expectedTx.FinalAmount, tx.FinalAmount, "Final Amount should match")
		assert.Equal(t, expectedTx.CreatedAt, tx.CreatedAt, "Created At should match")
	}

	t.Log("client outputs verified successfully. Now checking logs...")

	out, err := runCommand("docker", "compose", "logs", "aggregator")
	if err != nil {
		t.Fatalf("Failed to get docker compose logs: %v", err)
	}

	// Regex to find the full line, e.g. "Client [123e4567-e89b-12d3-a456-426614174000] finished."
	re := regexp.MustCompile(`Client \[.*?\] finished\.`)
	match := re.FindString(out)

	if match == "" {
		t.Fatalf("Expected log not found: pattern 'Client [<id>] finished.'\nLogs:\n%s", out)
	}

	t.Logf("Success! Found log: %s", match)
	clientConnection.Close()

}
