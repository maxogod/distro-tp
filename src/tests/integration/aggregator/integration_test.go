package filter_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/filter/mock"
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
		t1AggregateMock,
		t2_1AggregateMock,
		t2_2AggregateMock,
		t3AggregateMock,
		t4AggregateMock,
	}

	// Run each test one by one
	for _, test := range tests {
		test(t)
	}
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)
	aggregatorOutputQueue.Delete()
}

func t1AggregateMock(t *testing.T) {
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)
	defer aggregatorOutputQueue.StopConsuming()
	defer aggregatorOutputQueue.Close()

	// 	serializedTransactions, _ := proto.Marshal(&MockTransactionsBatch)
	// 	dataEnvelope := protocol.DataEnvelope{
	// 		ClientId: "test-client1",
	// 		TaskType: int32(enum.T1),
	// 		Payload:  serializedTransactions,
	// 	}
	// 	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	// 	filterQueue.Send(serializedDataEnvelope)

	// 	res := &raw.TransactionBatch{}
	// 	done := make(chan bool, 1)
	// 	e := aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
	// 		for msg := range consumeChannel {
	// 			msg.Ack(false)

	// 			dataBatch, _ := utils.GetDataEnvelope(msg.Body)

	// 			TransactionData := &raw.TransactionBatch{}
	// 			err := proto.Unmarshal(dataBatch.Payload, TransactionData)
	// 			assert.Nil(t, err)

	//			res.Transactions = TransactionData.Transactions
	//			break
	//		}
	//		done <- true
	//	})
	//
	// select {
	// case <-done:
	// case <-time.After(5 * time.Second):
	//
	//		t.Error("Test timed out waiting for results")
	//	}
	//
	// assert.Equal(t, 0, int(e))
	// assert.Equal(t, 3, len(res.Transactions), "Expected 2 transactions after filtering")
	// assert.Equal(t, "1", res.Transactions[0].TransactionId)
	// assert.Equal(t, "6", res.Transactions[1].TransactionId)
	// assert.Equal(t, "7", res.Transactions[2].TransactionId)
}

func t2_1AggregateMock(t *testing.T) {
}

func t2_2AggregateMock(t *testing.T) {
}

func t3AggregateMock(t *testing.T) {
}

func t4AggregateMock(t *testing.T) {
}
