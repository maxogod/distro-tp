package aggregator_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/aggregator/mock"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestMain(m *testing.M) {
	go mock.StartAggregatorMock("./config_test.yaml")
	logger.InitLogger(logger.LoggerEnvDevelopment)
	m.Run()
}

// TestSequentialRun runs tests in sequence to
// avoid consuming conflicts on the same queues.
func TestSequentialRun(t *testing.T) {
	tests := []func(t *testing.T){
		t2AggregateMock,
		t3AggregateMock,
		t4AggregateMock,
	}

	// Run each test one by one
	for _, test := range tests {
		test(t)
	}
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)}, "")
	aggregatorInputQueue := middleware.GetAggregatorQueue(url, "")
	joinerQueue := middleware.GetJoinerQueue(url)
	processedDataQueue := middleware.GetProcessedDataExchange(url, "none")
	finishExchange.Delete()
	aggregatorInputQueue.Delete()
	joinerQueue.Delete()
	processedDataQueue.Delete()
}

func t2AggregateMock(t *testing.T) {
	aggregatorInputQueue := middleware.GetAggregatorQueue(url, "")
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)}, "")
	clientID := "test-client-2"
	joinerOutputQueue := middleware.GetJoinerQueue(url)

	var totalSumItems []*reduced.TotalSumItem
	done := make(chan bool, 1)
	// each message should contain the grouped items
	e := joinerOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			totalSumBatch := &reduced.TotalSumItemsBatch{}
			err := proto.Unmarshal(dataBatch.Payload, totalSumBatch)
			assert.Nil(t, err)
			totalSumItems = append(totalSumItems, totalSumBatch.TotalSumItems...)
			break
		}
		done <- true
	})

	// Send T2 data to aggregator
	serializedTPV, _ := proto.Marshal(&MockTotalSumItems)
	dataEnvelope := protocol.DataEnvelope{
		ClientId: clientID,
		TaskType: int32(enum.T2),
		Payload:  serializedTPV,
	}
	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	aggregatorInputQueue.Send(serializedDataEnvelope)

	// Send done message to aggregator
	doneMessage := &protocol.DataEnvelope{
		ClientId: clientID,
		IsDone:   true,
		TaskType: int32(enum.T2),
	}
	doneBytes, _ := proto.Marshal(doneMessage)
	time.Sleep(3 * time.Second)
	err := finishExchange.Send(doneBytes)
	assert.Equal(t, err, middleware.MessageMiddlewareSuccess)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Timeout expected, no data received
	}
	assert.Equal(t, 0, int(e))

	t.Logf("Total sum items: %v", totalSumItems)

	assert.Equal(t, 0, len(totalSumItems), "Expected 0 items after aggregating")

	joinerOutputQueue.StopConsuming()
	joinerOutputQueue.Close()
	aggregatorInputQueue.Close()
	finishExchange.Close()
}

func t3AggregateMock(t *testing.T) {
	aggregatorInputQueue := middleware.GetAggregatorQueue(url, "")
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)}, "")
	clientID := "test-client-3"
	joinerOutputQueue := middleware.GetJoinerQueue(url)

	var tpvItems []*reduced.TotalPaymentValue
	done := make(chan bool, 1)
	// each message should contain the grouped items
	e := joinerOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			tpvItem := &reduced.TotalPaymentValueBatch{}
			err := proto.Unmarshal(dataBatch.Payload, tpvItem)
			assert.Nil(t, err)
			tpvItems = append(tpvItems, tpvItem.TotalPaymentValues...)
			break
		}
		done <- true
	})

	// Send T3 data to aggregator
	serializedTPV, _ := proto.Marshal(&MockTPV)
	dataEnvelope := protocol.DataEnvelope{
		ClientId: clientID,
		TaskType: int32(enum.T3),
		Payload:  serializedTPV,
	}
	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	aggregatorInputQueue.Send(serializedDataEnvelope)

	// Send done message to aggregator
	doneMessage := &protocol.DataEnvelope{
		ClientId: clientID,
		IsDone:   true,
		TaskType: int32(enum.T3),
	}
	doneBytes, _ := proto.Marshal(doneMessage)
	time.Sleep(3 * time.Second)
	err := finishExchange.Send(doneBytes)
	assert.Equal(t, err, middleware.MessageMiddlewareSuccess)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Timeout expected, no data received
	}
	assert.Equal(t, 0, int(e))

	assert.Equal(t, 2, len(tpvItems), "Expected 2 TPV items after aggregating")
	joinerOutputQueue.StopConsuming()
	joinerOutputQueue.Close()
	aggregatorInputQueue.Close()
	finishExchange.Close()
}

func t4AggregateMock(t *testing.T) {
	aggregatorInputQueue := middleware.GetAggregatorQueue(url, "")
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)}, "")
	clientID := "test-client-4"
	joinerOutputQueue := middleware.GetJoinerQueue(url)

	var countedUserTransactions []*reduced.CountedUserTransactions
	done := make(chan bool, 1)
	// each message should contain the grouped items
	e := joinerOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			countedUserTransaction := &reduced.CountedUserTransactionBatch{}
			err := proto.Unmarshal(dataBatch.Payload, countedUserTransaction)

			assert.Nil(t, err)

			countedUserTransactions = append(countedUserTransactions, countedUserTransaction.CountedUserTransactions...)
			break
		}
		done <- true
	})

	// Send T4 data to aggregator
	serializedCU, _ := proto.Marshal(&MockUsersDupQuantities)
	dataEnvelope := protocol.DataEnvelope{
		ClientId: clientID,
		TaskType: int32(enum.T4),
		Payload:  serializedCU,
	}
	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	aggregatorInputQueue.Send(serializedDataEnvelope)

	// Send done message to aggregator
	doneMessage := &protocol.DataEnvelope{
		ClientId: clientID,
		IsDone:   true,
		TaskType: int32(enum.T4),
	}
	doneBytes, _ := proto.Marshal(doneMessage)
	time.Sleep(3 * time.Second)
	err := finishExchange.Send(doneBytes)
	assert.Equal(t, err, middleware.MessageMiddlewareSuccess)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Timeout expected, no data received
	}
	assert.Equal(t, 0, int(e))

	assert.Equal(t, 2, len(countedUserTransactions), "Expected 2 user transactions after aggregating")

	joinerOutputQueue.StopConsuming()
	joinerOutputQueue.Close()
	aggregatorInputQueue.Close()
	finishExchange.Close()
}
