package joiner_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/joiner/mock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestMain(m *testing.M) {
	go mock.StartJoinerMock("./config_test.yaml")
	m.Run()
}

// TestSequentialRun runs tests in sequence to
// avoid consuming conflicts on the same queues.
func TestSequentialRun(t *testing.T) {
	tests := []func(t *testing.T){
		t2JoinerMock,
		t3JoinerMock,
		t4JoinerMock,
	}

	// Run each test one by one
	for _, test := range tests {
		test(t)
	}
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.JoinerWorker)})
	joinerInputQueue := middleware.GetJoinerQueue(url)
	aggregatorOutputQueue := middleware.GetProcessedDataExchange(url, "none")
	finishExchange.Delete()
	joinerInputQueue.Delete()
	aggregatorOutputQueue.Delete()
}

func t2JoinerMock(t *testing.T) {
	joinerInputQueue := middleware.GetJoinerQueue(url)
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.JoinerWorker)})
	clientID := "test-client-2"
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	// --- Send T2 related data references to joiner ---
	menuItemsBatch := &raw.MenuItemsBatch{
		MenuItems: MockMenuItems,
	}
	serializedMI, _ := proto.Marshal(menuItemsBatch)

	referenceEnvelope := &protocol.ReferenceEnvelope{
		Payload:       serializedMI,
		ReferenceType: int32(enum.MenuItems),
	}
	referenceBytes, _ := proto.Marshal(referenceEnvelope)
	referenceDataEnvelope := &protocol.DataEnvelope{
		ClientId: clientID,
		IsRef:    true,
		TaskType: int32(enum.T2),
		Payload:  referenceBytes,
	}
	referenceDataEnvelopeBytes, _ := proto.Marshal(referenceDataEnvelope)
	e := joinerInputQueue.Send(referenceDataEnvelopeBytes)
	assert.Equal(t, e, middleware.MessageMiddlewareSuccess)

	doneReferenceDataEnvelope := &protocol.DataEnvelope{
		ClientId: clientID,
		IsRef:    true,
		TaskType: int32(enum.T2),
		IsDone:   true,
	}
	doneReferenceDataEnvelopeBytes, _ := proto.Marshal(doneReferenceDataEnvelope)
	e = joinerInputQueue.Send(doneReferenceDataEnvelopeBytes)
	assert.Equal(t, e, middleware.MessageMiddlewareSuccess)

	// --- Send T2 data to joiner ---

	// This is T2_1
	for _, ts := range MockTotalProfit {
		serializedTS, _ := proto.Marshal(ts)
		dataEnvelope := protocol.DataEnvelope{
			ClientId: clientID,
			TaskType: int32(enum.T2_1),
			Payload:  serializedTS,
		}
		serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)
		joinerInputQueue.Send(serializedDataEnvelope)
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
		joinerInputQueue.Send(serializedDataEnvelope)
	}

	tsItems := []*reduced.TotalProfitBySubtotal{}
	tqItems := []*reduced.TotalSoldByQuantity{}

	doneCounter := len(MockTotalProfitOutput) + len(MockTotalSalesOutput)

	done := make(chan bool, 1)
	e = aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		t.Log("Starting to consume messages for T2")
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)

			if dataBatch.TaskType == int32(enum.T2_2) {
				tq := &reduced.TotalSoldByQuantity{}
				err := proto.Unmarshal(dataBatch.Payload, tq)
				assert.Nil(t, err)
				tqItems = append(tqItems, tq)
			} else if dataBatch.TaskType == int32(enum.T2_1) {
				ts := &reduced.TotalProfitBySubtotal{}
				err := proto.Unmarshal(dataBatch.Payload, ts)
				assert.Nil(t, err)
				tsItems = append(tsItems, ts)
			}

			doneCounter--
			if doneCounter == 0 {
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
	assert.Equal(t, len(MockTotalProfitOutput), len(tsItems), "Expected same amount of Total profit items after joining")
	assert.Equal(t, len(MockTotalSalesOutput), len(tqItems), "Expected same amount of Total quantity items after joining")

	for i, ts := range tsItems {
		assert.Equal(t, MockTotalProfitOutput[i].ItemId, ts.ItemId)
		assert.Equal(t, MockTotalProfitOutput[i].YearMonth, ts.YearMonth)
		assert.Equal(t, MockTotalProfitOutput[i].Subtotal, ts.Subtotal)
	}

	for i, tq := range tqItems {
		assert.Equal(t, MockTotalSalesOutput[i].ItemId, tq.ItemId)
		assert.Equal(t, MockTotalSalesOutput[i].YearMonth, tq.YearMonth)
		assert.Equal(t, MockTotalSalesOutput[i].Quantity, tq.Quantity)
	}

	aggregatorOutputQueue.StopConsuming()
	aggregatorOutputQueue.Close()
	joinerInputQueue.Close()
	finishExchange.Close()
}

func t3JoinerMock(t *testing.T) {
	joinerInputQueue := middleware.GetJoinerQueue(url)
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.JoinerWorker)})
	clientID := "test-client-3"
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	// --- Send T3 related data references to joiner ---
	storeBatch := &raw.StoreBatch{
		Stores: MockStores,
	}
	serializedStores, _ := proto.Marshal(storeBatch)

	referenceEnvelope := &protocol.ReferenceEnvelope{
		Payload:       serializedStores,
		ReferenceType: int32(enum.Stores),
	}
	referenceBytes, _ := proto.Marshal(referenceEnvelope)
	referenceDataEnvelope := &protocol.DataEnvelope{
		ClientId: clientID,
		IsRef:    true,
		TaskType: int32(enum.T3),
		Payload:  referenceBytes,
	}
	referenceDataEnvelopeBytes, _ := proto.Marshal(referenceDataEnvelope)
	e := joinerInputQueue.Send(referenceDataEnvelopeBytes)
	assert.Equal(t, e, middleware.MessageMiddlewareSuccess)

	doneReferenceDataEnvelope := &protocol.DataEnvelope{
		ClientId: clientID,
		IsRef:    true,
		TaskType: int32(enum.T3),
		IsDone:   true,
	}
	doneReferenceDataEnvelopeBytes, _ := proto.Marshal(doneReferenceDataEnvelope)
	e = joinerInputQueue.Send(doneReferenceDataEnvelopeBytes)
	assert.Equal(t, e, middleware.MessageMiddlewareSuccess)

	// --- Send T3 data to joiner ---

	for _, tpv := range MockTPV {
		serializedTPV, _ := proto.Marshal(tpv)
		dataEnvelope := protocol.DataEnvelope{
			ClientId: clientID,
			TaskType: int32(enum.T3),
			Payload:  serializedTPV,
		}
		serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)
		joinerInputQueue.Send(serializedDataEnvelope)
	}

	tpvItems := []*reduced.TotalPaymentValue{}

	doneCounter := len(MockTpvOutput)

	done := make(chan bool, 1)
	e = aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		t.Log("Starting to consume messages for T3")
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)

			tpv := &reduced.TotalPaymentValue{}
			err := proto.Unmarshal(dataBatch.Payload, tpv)
			assert.Nil(t, err)
			tpvItems = append(tpvItems, tpv)

			doneCounter--
			if doneCounter == 0 {
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
	assert.Equal(t, len(MockTpvOutput), len(tpvItems), "Expected same amount of Total profit items after joining")

	for i, tq := range tpvItems {
		assert.Equal(t, MockTpvOutput[i].Semester, tq.Semester)
		assert.Equal(t, MockTpvOutput[i].StoreId, tq.StoreId)
		assert.Equal(t, MockTpvOutput[i].FinalAmount, tq.FinalAmount)
	}

	aggregatorOutputQueue.StopConsuming()
	aggregatorOutputQueue.Close()
	joinerInputQueue.Close()
	finishExchange.Close()
}

func t4JoinerMock(t *testing.T) {
	joinerInputQueue := middleware.GetJoinerQueue(url)
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.JoinerWorker)})
	clientID := "test-client-4"
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	// --- Send T4 related data references to joiner ---
	// Send Stores:
	storeBatch := &raw.StoreBatch{
		Stores: MockStores,
	}
	serializedStores, _ := proto.Marshal(storeBatch)

	referenceEnvelope := &protocol.ReferenceEnvelope{
		Payload:       serializedStores,
		ReferenceType: int32(enum.Stores),
	}
	referenceBytes, _ := proto.Marshal(referenceEnvelope)
	referenceDataEnvelope := &protocol.DataEnvelope{
		ClientId: clientID,
		IsRef:    true,
		TaskType: int32(enum.T4),
		Payload:  referenceBytes,
	}
	referenceDataEnvelopeBytes, _ := proto.Marshal(referenceDataEnvelope)
	e := joinerInputQueue.Send(referenceDataEnvelopeBytes)
	assert.Equal(t, e, middleware.MessageMiddlewareSuccess)

	// Send Users:
	userBatch := &raw.UserBatch{
		Users: MockUsers,
	}
	serializedUsers, _ := proto.Marshal(userBatch)

	referenceEnvelope2 := &protocol.ReferenceEnvelope{
		Payload:       serializedUsers,
		ReferenceType: int32(enum.Users),
	}
	referenceBytes2, _ := proto.Marshal(referenceEnvelope2)
	referenceDataEnvelope2 := &protocol.DataEnvelope{
		ClientId: clientID,
		IsRef:    true,
		TaskType: int32(enum.T4),
		Payload:  referenceBytes2,
	}
	referenceDataEnvelopeBytes2, _ := proto.Marshal(referenceDataEnvelope2)
	e = joinerInputQueue.Send(referenceDataEnvelopeBytes2)
	assert.Equal(t, e, middleware.MessageMiddlewareSuccess)

	// Send Done ref
	doneReferenceDataEnvelope := &protocol.DataEnvelope{
		ClientId: clientID,
		IsRef:    true,
		TaskType: int32(enum.T4),
		IsDone:   true,
	}
	doneReferenceDataEnvelopeBytes, _ := proto.Marshal(doneReferenceDataEnvelope)
	e = joinerInputQueue.Send(doneReferenceDataEnvelopeBytes)
	assert.Equal(t, e, middleware.MessageMiddlewareSuccess)

	// --- Send T4 data to joiner ---

	for _, ct := range MockCountedUserTransactions {
		serializedCT, _ := proto.Marshal(ct)
		dataEnvelope := protocol.DataEnvelope{
			ClientId: clientID,
			TaskType: int32(enum.T4),
			Payload:  serializedCT,
		}
		serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)
		joinerInputQueue.Send(serializedDataEnvelope)
	}

	countedTransactionItems := []*reduced.CountedUserTransactions{}

	doneCounter := len(MockCountedUserTransactionsOutput)

	done := make(chan bool, 1)
	e = aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		t.Log("Starting to consume messages for T4")
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)

			ct := &reduced.CountedUserTransactions{}
			err := proto.Unmarshal(dataBatch.Payload, ct)
			assert.Nil(t, err)
			countedTransactionItems = append(countedTransactionItems, ct)

			doneCounter--
			if doneCounter == 0 {
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
	assert.Equal(t, len(MockCountedUserTransactionsOutput), len(countedTransactionItems), "Expected same amount of Total profit items after joining")

	for i, tq := range countedTransactionItems {
		assert.Equal(t, MockCountedUserTransactionsOutput[i].UserId, tq.UserId)
		assert.Equal(t, MockCountedUserTransactionsOutput[i].Birthdate, tq.Birthdate)
		assert.Equal(t, MockCountedUserTransactionsOutput[i].StoreId, tq.StoreId)
	}

	aggregatorOutputQueue.StopConsuming()
	aggregatorOutputQueue.Close()
	joinerInputQueue.Close()
	finishExchange.Close()
}

func t3BufferMock(t *testing.T) {
	joinerInputQueue := middleware.GetJoinerQueue(url)
	finishExchange := middleware.GetFinishExchange(url, []string{string(enum.JoinerWorker)})
	clientID := "test-buffer-3"
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	// --- Send T3 related data references to joiner ---
	storeBatch := &raw.StoreBatch{
		Stores: MockStores,
	}
	serializedStores, _ := proto.Marshal(storeBatch)

	referenceEnvelope := &protocol.ReferenceEnvelope{
		Payload:       serializedStores,
		ReferenceType: int32(enum.Stores),
	}
	referenceBytes, _ := proto.Marshal(referenceEnvelope)
	referenceDataEnvelope := &protocol.DataEnvelope{
		ClientId: clientID,
		IsRef:    true,
		TaskType: int32(enum.T3),
		Payload:  referenceBytes,
	}
	referenceDataEnvelopeBytes, _ := proto.Marshal(referenceDataEnvelope)
	e := joinerInputQueue.Send(referenceDataEnvelopeBytes)
	assert.Equal(t, e, middleware.MessageMiddlewareSuccess)

	doneReferenceDataEnvelope := &protocol.DataEnvelope{
		ClientId: clientID,
		IsRef:    true,
		TaskType: int32(enum.T3),
		IsDone:   true,
	}
	doneReferenceDataEnvelopeBytes, _ := proto.Marshal(doneReferenceDataEnvelope)
	e = joinerInputQueue.Send(doneReferenceDataEnvelopeBytes)
	assert.Equal(t, e, middleware.MessageMiddlewareSuccess)

	// --- Send T3 data to joiner ---

	for _, tpv := range MockTPV {
		serializedTPV, _ := proto.Marshal(tpv)
		dataEnvelope := protocol.DataEnvelope{
			ClientId: clientID,
			TaskType: int32(enum.T3),
			Payload:  serializedTPV,
		}
		serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)
		joinerInputQueue.Send(serializedDataEnvelope)
	}

	tpvItems := []*reduced.TotalPaymentValue{}

	doneCounter := len(MockTpvOutput)

	done := make(chan bool, 1)
	e = aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		t.Log("Starting to consume messages for T3")
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)

			tpv := &reduced.TotalPaymentValue{}
			err := proto.Unmarshal(dataBatch.Payload, tpv)
			assert.Nil(t, err)
			tpvItems = append(tpvItems, tpv)

			doneCounter--
			if doneCounter == 0 {
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
	assert.Equal(t, len(MockTpvOutput), len(tpvItems), "Expected same amount of Total profit items after joining")

	for i, tq := range tpvItems {
		assert.Equal(t, MockTpvOutput[i].Semester, tq.Semester)
		assert.Equal(t, MockTpvOutput[i].StoreId, tq.StoreId)
		assert.Equal(t, MockTpvOutput[i].FinalAmount, tq.FinalAmount)
	}

	aggregatorOutputQueue.StopConsuming()
	aggregatorOutputQueue.Close()
	joinerInputQueue.Close()
	finishExchange.Close()
}
