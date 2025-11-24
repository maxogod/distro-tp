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

type clientData struct {
	clientID   string
	connection network.ConnectionInterface
}

func testNtoM(t *testing.T) {
	t.Log("Starting N-to-M concurrent test with output collection")

	numClients := 3
	clients := make([]clientData, numClients)
	defer func() {
		for _, c := range clients {
			if c.connection != nil {
				c.connection.Close()
			}
		}
	}()

	// --- Connect all clients ---
	for i := 0; i < numClients; i++ {
		client := network.NewConnection()
		if err := client.Connect(address, 5); err != nil {
			t.Fatalf("Client %d failed to connect: %v", i+1, err)
		}
		clients[i] = clientData{
			clientID:   "",
			connection: client,
		}
	}
	t.Logf("All %d clients connected successfully.", numClients)

	// --- Channel to collect outputs ---
	outputCh := make(chan map[string]*reduced.TotalPaymentValue, numClients)

	// --- Run clients concurrently ---
	for i := range clients {
		go func(i int) {
			client := &clients[i]

			// --- Send store reference data ---
			storeRefDataBytes := getStoreDataBytes(t)
			client.connection.SendData(storeRefDataBytes)

			doneRef := getEOFDataBytes(t, enum.T3, true)
			client.connection.SendData(doneRef)

			// --- Send transactions batch ---
			dataBytes := getDataBytes(t, &mock.MockTransactionsBatchT3, enum.T3)
			client.connection.SendData(dataBytes)

			doneData := getEOFDataBytes(t, enum.T3, false)
			client.connection.SendData(doneData)

			t.Logf("Client %d waiting for output", i+1)
			output := map[string]*reduced.TotalPaymentValue{}
			for {
				data, _ := client.connection.ReceiveData()

				envelope := protocol.DataEnvelope{}
				proto.Unmarshal(data, &envelope)
				if envelope.IsDone {
					t.Logf("client %d has received all of their data", i+1)
					break
				}
				tpv := reduced.TotalPaymentValue{}
				proto.Unmarshal(envelope.Payload, &tpv)
				client.clientID = envelope.ClientId
				output[tpv.StoreId] = &tpv
			}
			// Send output to channel
			outputCh <- output
		}(i)
	}

	// --- Collect outputs ---
	allOutputs := make([]map[string]*reduced.TotalPaymentValue, 0, numClients)
	receivedCount := 0

	for receivedCount < numClients {
		out := <-outputCh
		allOutputs = append(allOutputs, out)
		receivedCount++
	}

	t.Log("All clients finished and output collected.")

	for i, output := range allOutputs {
		assert.Equal(t, len(mock.MockTPVOutput), len(output), "Client %d: expected output length", i+1)
		for j, tx := range output {
			expectedTx := mock.MockTPVOutput[j]
			assert.Equal(t, expectedTx.StoreId, tx.StoreId, "Client %d: Store ID", i+1)
			assert.Equal(t, expectedTx.Semester, tx.Semester, "Client %d: Semester", i+1)
			assert.Equal(t, expectedTx.FinalAmount, tx.FinalAmount, "Client %d: Final Amount", i+1)
		}
	}
	t.Log("All client outputs verified successfully. Now checking logs...")

	for i, client := range clients {
		assert.True(t, checkEOFLog(t, "aggregator", client.clientID), "Client %d: EOF log not found in aggregator container", i+1)
		assert.True(t, checkEOFLog(t, "joiner1", client.clientID), "Client %d: EOF log not found in joiner1 container", i+1)
		assert.True(t, checkEOFLog(t, "joiner2", client.clientID), "Client %d: EOF log not found in joiner2 container", i+1)
	}

}
