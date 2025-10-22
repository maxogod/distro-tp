package filter_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/filter/mock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestMain(m *testing.M) {
	go mock.StartFilterMock("./config_test.yaml")
	m.Run()
}

// TestSequentialRun runs tests in sequence to
// avoid consuming conflicts on the same queues.
func TestSequentialRun(t *testing.T) {
	tests := []func(t *testing.T){
		t1FilterMock,
		t2FilterMock,
		t3FilterMock,
		t4FilterMock,
	}

	// Run each test one by one
	for _, test := range tests {
		test(t)
	}

	filterQueue := middleware.GetFilterQueue(url)
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)
	groupbyOutputQueue := middleware.GetGroupByQueue(url)
	filterQueue.Delete()
	aggregatorOutputQueue.Delete()
	groupbyOutputQueue.Delete()
}

func t1FilterMock(t *testing.T) {
	filterQueue := middleware.GetFilterQueue(url)
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	serializedTransactions, _ := proto.Marshal(&MockTransactionsBatch)
	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client1",
		TaskType: int32(enum.T1),
		Payload:  serializedTransactions,
	}
	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	filterQueue.Send(serializedDataEnvelope)

	res := &raw.TransactionBatch{}
	done := make(chan bool, 1)
	e := aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)

			dataBatch, _ := utils.GetDataEnvelope(msg.Body)

			TransactionData := &raw.TransactionBatch{}
			err := proto.Unmarshal(dataBatch.Payload, TransactionData)
			assert.Nil(t, err)

			res.Transactions = TransactionData.Transactions
			break
		}
		done <- true
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Test timed out waiting for results")
	}
	assert.Equal(t, 0, int(e))
	assert.Equal(t, 3, len(res.Transactions), "Expected 2 transactions after filtering")
	assert.Equal(t, "1", res.Transactions[0].TransactionId)
	assert.Equal(t, "6", res.Transactions[1].TransactionId)
	assert.Equal(t, "7", res.Transactions[2].TransactionId)

	aggregatorOutputQueue.StopConsuming()
	filterQueue.Close()
	aggregatorOutputQueue.Close()
}

func t2FilterMock(t *testing.T) {
	filterQueue := middleware.GetFilterQueue(url)
	groupbyOutputQueue := middleware.GetGroupByQueue(url)

	serializedTransactions, _ := proto.Marshal(&MockTransactionItemsBatch)
	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client2",
		TaskType: int32(enum.T2),
		Payload:  serializedTransactions,
	}
	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	filterQueue.Send(serializedDataEnvelope)

	res := &raw.TransactionItemsBatch{}
	done := make(chan bool, 1)
	e := groupbyOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)

			dataBatch, _ := utils.GetDataEnvelope(msg.Body)

			TransactionData := &raw.TransactionItemsBatch{}
			err := proto.Unmarshal(dataBatch.Payload, TransactionData)
			assert.Nil(t, err)

			res.TransactionItems = TransactionData.TransactionItems
			break
		}
		done <- true
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Test timed out waiting for results")
	}
	assert.Equal(t, 0, int(e))
	assert.Equal(t, 6, len(res.TransactionItems), "Expected 6 transactions after filtering")
	assert.Equal(t, "1", res.TransactionItems[0].ItemId)
	assert.Equal(t, "2", res.TransactionItems[1].ItemId)
	assert.Equal(t, "3", res.TransactionItems[2].ItemId)
	assert.Equal(t, "4", res.TransactionItems[3].ItemId)
	assert.Equal(t, "5", res.TransactionItems[4].ItemId)
	assert.Equal(t, "6", res.TransactionItems[5].ItemId)

	groupbyOutputQueue.StopConsuming()
	filterQueue.Close()
	groupbyOutputQueue.Close()
}

func t3FilterMock(t *testing.T) {
	filterQueue := middleware.GetFilterQueue(url)
	groupbyOutputQueue := middleware.GetGroupByQueue(url)

	serializedTransactions, _ := proto.Marshal(&MockTransactionsBatch)
	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client3",
		TaskType: int32(enum.T3),
		Payload:  serializedTransactions,
	}
	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	filterQueue.Send(serializedDataEnvelope)

	res := &raw.TransactionBatch{}
	done := make(chan bool, 1)
	e := groupbyOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)

			dataBatch, _ := utils.GetDataEnvelope(msg.Body)

			TransactionData := &raw.TransactionBatch{}
			err := proto.Unmarshal(dataBatch.Payload, TransactionData)
			assert.Nil(t, err)

			res.Transactions = TransactionData.Transactions
			break
		}
		done <- true
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Test timed out waiting for results")
	}
	assert.Equal(t, 0, int(e))
	assert.Equal(t, 4, len(res.Transactions), "Expected 4 transactions after filtering")
	assert.Equal(t, "1", res.Transactions[0].TransactionId)
	assert.Equal(t, "4", res.Transactions[1].TransactionId)
	assert.Equal(t, "6", res.Transactions[2].TransactionId)
	assert.Equal(t, "7", res.Transactions[3].TransactionId)

	groupbyOutputQueue.StopConsuming()
	filterQueue.Close()
	groupbyOutputQueue.Close()
}

func t4FilterMock(t *testing.T) {
	filterQueue := middleware.GetFilterQueue(url)
	groupbyOutputQueue := middleware.GetGroupByQueue(url)

	serializedTransactions, _ := proto.Marshal(&MockTransactionsBatch)
	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client4",
		TaskType: int32(enum.T4),
		Payload:  serializedTransactions,
	}
	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	filterQueue.Send(serializedDataEnvelope)

	res := &raw.TransactionBatch{}
	done := make(chan bool, 1)
	e := groupbyOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			t.Log("Message received")
			msg.Ack(false)

			dataBatch, _ := utils.GetDataEnvelope(msg.Body)

			TransactionData := &raw.TransactionBatch{}
			err := proto.Unmarshal(dataBatch.Payload, TransactionData)
			assert.Nil(t, err)

			res.Transactions = TransactionData.Transactions
			break
		}
		done <- true
	})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Test timed out waiting for results")
	}
	assert.Equal(t, 0, int(e))
	assert.Equal(t, 5, len(res.Transactions), "Expected 5 transactions after filtering")
	assert.Equal(t, "2", res.Transactions[0].TransactionId)
	assert.Equal(t, "3", res.Transactions[1].TransactionId)
	assert.Equal(t, "4", res.Transactions[2].TransactionId)
	assert.Equal(t, "6", res.Transactions[3].TransactionId)
	assert.Equal(t, "7", res.Transactions[4].TransactionId)

	groupbyOutputQueue.StopConsuming()
	filterQueue.Close()
	groupbyOutputQueue.Close()
}
