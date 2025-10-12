package filter_test

import (
	"testing"
	"time"

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
		t2AggregateMock,
		t3AggregateMock,
		t4AggregateMock,
	}

	// Run each test one by one
	for _, test := range tests {
		test(t)
	}
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)})
	aggregatorInputQueue := middleware.GetAggregatorQueue(url)
	processedDataQueue := middleware.GetProcessedDataExchange(url, "none")
	finishExchange.Delete()
	aggregatorInputQueue.Delete()
	processedDataQueue.Delete()
}

func t1AggregateMock(t *testing.T) {
	aggregatorInputQueue := middleware.GetAggregatorQueue(url)
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)})
	clientID := "test-client-1"
	processedDataQueue := middleware.GetProcessedDataExchange(url, clientID)

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

			if dataBatch.IsDone {
				break
			}

			transactionBatch := &raw.TransactionBatch{}
			err := proto.Unmarshal(dataBatch.Payload, transactionBatch)

			assert.Nil(t, err)

			transactions = append(transactions, transactionBatch.Transactions...)

		}
		done <- true
	})
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Error("Test timed out waiting for results")
	}
	assert.Equal(t, 0, int(e))

	assert.Equal(t, 3, len(transactions), "Expected 3 transactions after aggregating")
	assert.Equal(t, MockTransactionsBatch.GetTransactions()[0].TransactionId, transactions[0].TransactionId)
	assert.Equal(t, MockTransactionsBatch.GetTransactions()[1].TransactionId, transactions[1].TransactionId)
	assert.Equal(t, MockTransactionsBatch.GetTransactions()[2].TransactionId, transactions[2].TransactionId)

	processedDataQueue.StopConsuming()
	processedDataQueue.Close()
	aggregatorInputQueue.Close()
	finishExchange.Close()
}

func t2AggregateMock(t *testing.T) {
	aggregatorInputQueue := middleware.GetAggregatorQueue(url)
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)})
	clientID := "test-client-2"
	processedDataQueue := middleware.GetProcessedDataExchange(url, clientID)

	// Send T2 data to aggregator

	// This is T2_1
	for _, ts := range MockTotalProfit {
		serializedTS, _ := proto.Marshal(ts)
		dataEnvelope := protocol.DataEnvelope{
			ClientId: clientID,
			TaskType: int32(enum.T2_1),
			Payload:  serializedTS,
		}
		serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)
		aggregatorInputQueue.Send(serializedDataEnvelope)
	}

	// This is T2_2
	for _, tq := range MockTotalSales {
		serializedTS, _ := proto.Marshal(tq)
		dataEnvelope := protocol.DataEnvelope{
			ClientId: clientID,
			TaskType: int32(enum.T2_2),
			Payload:  serializedTS,
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

	tsItems := []*reduced.TotalProfitBySubtotal{}
	tqItems := []*reduced.TotalSoldByQuantity{}

	doneCounter := 0

	done := make(chan bool, 1)
	e := processedDataQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		t.Log("Starting to consume messages for T2")
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)

			if doneCounter == 1 && !dataBatch.IsDone {
				tq := &reduced.TotalSoldByQuantity{}
				err := proto.Unmarshal(dataBatch.Payload, tq)
				assert.Nil(t, err)
				tqItems = append(tqItems, tq)
			} else if !dataBatch.IsDone {
				ts := &reduced.TotalProfitBySubtotal{}
				err := proto.Unmarshal(dataBatch.Payload, ts)
				assert.Nil(t, err)
				tsItems = append(tsItems, ts)
			}

			if dataBatch.IsDone {
				doneCounter++
			}

			if doneCounter == 2 {
				break
			}
		}
		done <- true
	})
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Error("Test timed out waiting for results")
	}
	assert.Equal(t, 0, int(e))

	assert.Equal(t, len(MockTotalProfitOutput), len(tsItems), "Expected 3 Total profit items after aggregating")
	assert.Equal(t, len(MockTotalQuantityOutput), len(tqItems), "Expected 3 Total quantity items after aggregating")
	for i, ts := range tsItems {
		assert.Equal(t, MockTotalProfitOutput[i].ItemId, ts.ItemId)
		assert.Equal(t, MockTotalProfitOutput[i].YearMonth, ts.YearMonth)
		assert.Equal(t, MockTotalProfitOutput[i].Subtotal, ts.Subtotal)
	}
	for i, tq := range tqItems {
		assert.Equal(t, MockTotalQuantityOutput[i].ItemId, tq.ItemId)
		assert.Equal(t, MockTotalQuantityOutput[i].YearMonth, tq.YearMonth)
		assert.Equal(t, MockTotalQuantityOutput[i].Quantity, tq.Quantity)
	}
	processedDataQueue.StopConsuming()
	processedDataQueue.Close()
	aggregatorInputQueue.Close()
	finishExchange.Close()
}

func t3AggregateMock(t *testing.T) {
	aggregatorInputQueue := middleware.GetAggregatorQueue(url)
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)})
	clientID := "test-client-3"
	processedDataQueue := middleware.GetProcessedDataExchange(url, clientID)

	// Send T3 data to aggregator

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
			if dataBatch.IsDone {
				break
			}
			tpvItem := &reduced.TotalPaymentValue{}
			err := proto.Unmarshal(dataBatch.Payload, tpvItem)
			assert.Nil(t, err)
			tpvItems = append(tpvItems, tpvItem)

		}
		done <- true
	})
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Error("Test timed out waiting for results")
	}
	assert.Equal(t, 0, int(e))

	assert.Equal(t, len(MockTpvOutput), len(tpvItems), "Expected 2 TPV items after aggregating")
	for i, tpv := range tpvItems {
		assert.Equal(t, MockTpvOutput[i].StoreId, tpv.StoreId)
		assert.Equal(t, MockTpvOutput[i].Semester, tpv.Semester)
		assert.Equal(t, MockTpvOutput[i].FinalAmount, tpv.FinalAmount)
	}
	processedDataQueue.StopConsuming()
	processedDataQueue.Close()
	aggregatorInputQueue.Close()
	finishExchange.Close()
}

func t4AggregateMock(t *testing.T) {
	aggregatorInputQueue := middleware.GetAggregatorQueue(url)
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.AggregatorWorker)})
	clientID := "test-client-4"
	processedDataQueue := middleware.GetProcessedDataExchange(url, clientID)

	// Send T4 data to aggregator

	for _, countedUsers := range MockUsersDupQuantities {
		serializedCU, _ := proto.Marshal(countedUsers)
		dataEnvelope := protocol.DataEnvelope{
			ClientId: clientID,
			TaskType: int32(enum.T4),
			Payload:  serializedCU,
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

	countedUserTransactions := []*reduced.CountedUserTransactions{}
	done := make(chan bool, 1)
	// each message should contain the grouped items
	e := processedDataQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			if dataBatch.IsDone {
				break
			}
			countedUserTransaction := &reduced.CountedUserTransactions{}
			err := proto.Unmarshal(dataBatch.Payload, countedUserTransaction)

			assert.Nil(t, err)

			countedUserTransactions = append(countedUserTransactions, countedUserTransaction)

		}
		done <- true
	})
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Error("Test timed out waiting for results")
	}
	assert.Equal(t, 0, int(e))

	countedTransactionsCounter := make(map[string](map[int32]int))

	for _, countedUserTransaction := range countedUserTransactions {
		storeID := countedUserTransaction.StoreId
		quantity := countedUserTransaction.TransactionQuantity
		if _, exists := countedTransactionsCounter[storeID]; !exists {
			countedTransactionsCounter[storeID] = make(map[int32]int)
		}
		countedTransactionsCounter[storeID][quantity]++
	}

	assert.Equal(t, len(MockUsersDupQuantitiesOutput), len(countedTransactionsCounter), "Expected The same amount of Counted User transactions after aggregating")

	for storeID, quantities := range MockUsersDupQuantitiesOutput {
		// Check if the storeID exists in countedTransactionsCounter
		if _, exists := countedTransactionsCounter[storeID]; !exists {
			t.Errorf("StoreID %s not found in countedTransactionsCounter", storeID)
			continue
		}

		// Check if the quantities match for the storeID
		for quantity, expectedCount := range quantities {
			actualCount, exists := countedTransactionsCounter[storeID][quantity]
			if !exists {
				t.Errorf("Quantity %d not found for StoreID %s in countedTransactionsCounter", quantity, storeID)
				continue
			}
			assert.Equal(t, expectedCount, actualCount, "Mismatch in transaction count for StoreID %s and Quantity %d", storeID, quantity)
		}
	}

	processedDataQueue.StopConsuming()
	processedDataQueue.Close()
	aggregatorInputQueue.Close()
	finishExchange.Close()
}
