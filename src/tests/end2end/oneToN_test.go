package eof_test

import (
	"testing"

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
	clientConnection := network.NewConnection()
	err := clientConnection.Connect(address, 5)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientConnection.Close()
	var clientID string

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
		clientID = receivedEnvelope.ClientId
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

	assert.True(t, checkEOFLog(t, "aggregator", clientID), "EOF log not found in aggregator container")
	assert.True(t, checkEOFLog(t, "joiner1", clientID), "EOF log not found in joiner1 container")
	assert.True(t, checkEOFLog(t, "joiner2", clientID), "EOF log not found in joiner2 container")

}
