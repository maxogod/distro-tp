package filter_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/aggregator/mock"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestMain(m *testing.M) {
	go mock.StartAggregatorMock("./config_test.yaml")
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
	// finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)})
	// aggregatorInputQueue := middleware.GetAggregatorQueue(url)
	// finishExchange.Delete()
	// aggregatorInputQueue.Delete()
}

func t1AggregateMock(t *testing.T) {
	aggregatorInputQueue := middleware.GetAggregatorQueue(url)
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)})
	clientID := "test-client-1"
	processedDataQueue := middleware.GetProcessedDataExchange(url, clientID)

	// defer aggregatorInputQueue.StopConsuming()
	// defer finishExchange.StopConsuming()
	// defer processedDataQueue.StopConsuming()
	// defer aggregatorInputQueue.Close()
	// defer finishExchange.Close()
	// defer processedDataQueue.Close()

	// Send T1 data to aggregator
	serializedTransactions, _ := proto.Marshal(&MockTransactionsBatch)
	dataEnvelope := protocol.DataEnvelope{
		ClientId: clientID,
		TaskType: int32(enum.T1),
		Payload:  serializedTransactions,
	}
	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	aggregatorInputQueue.Send(serializedDataEnvelope)

	// Send done message to aggregator
	doneMessage := &protocol.DataEnvelope{
		ClientId: clientID,
		IsDone:   true,
	}
	doneBytes, _ := proto.Marshal(doneMessage)
	err := finishExchange.Send(doneBytes)
	assert.Equal(t, err, middleware.MessageMiddlewareSuccess)

	transactions := []*raw.Transaction{}
	done := make(chan bool, 1)
	// each message should contain the grouped items
	e := processedDataQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T1)

			transactionBatch := &raw.TransactionBatch{}
			err := proto.Unmarshal(dataBatch.Payload, transactionBatch)

			assert.Nil(t, err)

			transactions = append(transactions, transactionBatch.Transactions...)

			if len(transactions) == len(MockTransactionsBatch.GetTransactions()) {
				break
			}

		}
		done <- true
	})
	<-done
	assert.Equal(t, 0, int(e))

	assert.Equal(t, 3, len(transactions), "Expected 3 transactions after filtering")
	assert.Equal(t, MockTransactionsBatch.GetTransactions()[0].TransactionId, transactions[0].TransactionId)
	assert.Equal(t, MockTransactionsBatch.GetTransactions()[1].TransactionId, transactions[1].TransactionId)
	assert.Equal(t, MockTransactionsBatch.GetTransactions()[2].TransactionId, transactions[2].TransactionId)
}

func t2_1AggregateMock(t *testing.T) {
}

func t2_2AggregateMock(t *testing.T) {
}

func t3AggregateMock(t *testing.T) {
	aggregatorInputQueue := middleware.GetAggregatorQueue(url)
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)})
	clientID := "test-client-3"
	processedDataQueue := middleware.GetProcessedDataExchange(url, clientID)

	// defer aggregatorInputQueue.StopConsuming()
	// defer finishExchange.StopConsuming()
	// defer processedDataQueue.StopConsuming()
	// defer aggregatorInputQueue.Close()
	// defer finishExchange.Close()
	// defer processedDataQueue.Close()

	// Send T1 data to aggregator

	for _, tpv := range MockTPV {
		serializedTPV, _ := proto.Marshal(tpv)
		dataEnvelope := protocol.DataEnvelope{
			ClientId: clientID,
			TaskType: int32(enum.T3),
			Payload:  serializedTPV,
		}
		serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

		aggregatorInputQueue.Send(serializedDataEnvelope)
	}

	// Send done message to aggregator
	doneMessage := &protocol.DataEnvelope{
		ClientId: clientID,
		IsDone:   true,
	}
	doneBytes, _ := proto.Marshal(doneMessage)
	err := finishExchange.Send(doneBytes)
	assert.Equal(t, err, middleware.MessageMiddlewareSuccess)

	tpvItems := []*reduced.TotalPaymentValue{}
	done := make(chan bool, 1)
	// each message should contain the grouped items
	e := processedDataQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T3)
			tpvItem := &reduced.TotalPaymentValue{}
			err := proto.Unmarshal(dataBatch.Payload, tpvItem)

			assert.Nil(t, err)

			tpvItems = append(tpvItems, tpvItem)

			if len(tpvItems) == len(MockTpvOutput) {
				break
			}

		}
		done <- true
	})
	<-done
	assert.Equal(t, 0, int(e))

	assert.Equal(t, len(MockTpvOutput), len(tpvItems), "Expected 2 TPV items after filtering")
	for i, tpv := range tpvItems {
		assert.Equal(t, MockTpvOutput[i].StoreId, tpv.StoreId)
		assert.Equal(t, MockTpvOutput[i].Semester, tpv.Semester)
		assert.Equal(t, MockTpvOutput[i].FinalAmount, tpv.FinalAmount)
	}
}

func t4AggregateMock(t *testing.T) {
}
