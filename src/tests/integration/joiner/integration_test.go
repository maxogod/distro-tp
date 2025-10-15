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
		//t3JoinerMock,
		//t4JoinerMock,
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
