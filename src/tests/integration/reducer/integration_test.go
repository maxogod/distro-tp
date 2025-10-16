package reducer_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/reducer/mock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestMain(m *testing.M) {
	go mock.StartReducerMock("./config_test.yaml")
	m.Run()
}

// TestSequentialRun runs tests in sequence to
// avoid consuming conflicts on the same queues.
func TestSequentialRun(t *testing.T) {
	tests := []func(t *testing.T){
		reduceTask2_1,
		reduceTask2_2,
		reduceTask3,
		reduceTask4,
	}

	// Run each test one by one
	for _, test := range tests {
		test(t)
	}

	groupbyOutputQueue := middleware.GetGroupByQueue(url)
	reducerOutputQueue := middleware.GetReducerQueue(url)
	joinerOutputQueue := middleware.GetJoinerQueue(url)
	groupbyOutputQueue.Delete()
	reducerOutputQueue.Delete()
	joinerOutputQueue.Delete()
}

func reduceTask3(t *testing.T) {
	reducerInputQueue := middleware.GetReducerQueue(url)
	joinerOutputQueue := middleware.GetJoinerQueue(url)
	defer reducerInputQueue.StopConsuming()
	defer joinerOutputQueue.StopConsuming()
	defer reducerInputQueue.Close()
	defer joinerOutputQueue.Close()

	serializedTransactions, _ := proto.Marshal(&GroupTransactionMock1)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client",
		TaskType: int32(enum.T3),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	reducerInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	e := joinerOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T3)

			reducedData := &reduced.TotalPaymentValue{}
			err := proto.Unmarshal(dataBatch.Payload, reducedData)

			assert.Nil(t, err)

			assert.Equal(t, ReducedTransactionMock1.GetFinalAmount(), reducedData.GetFinalAmount())
			assert.Equal(t, ReducedTransactionMock1.GetStoreId(), reducedData.GetStoreId())
			assert.Equal(t, ReducedTransactionMock1.GetSemester(), reducedData.GetSemester())

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
}

func reduceTask2_1(t *testing.T) {
	reducerInputQueue := middleware.GetReducerQueue(url)
	joinerOutputQueue := middleware.GetJoinerQueue(url)
	defer reducerInputQueue.StopConsuming()
	defer joinerOutputQueue.StopConsuming()
	defer reducerInputQueue.Close()
	defer joinerOutputQueue.Close()

	serializedTransactions, _ := proto.Marshal(&GroupTransactionMock2)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client",
		TaskType: int32(enum.T2_1),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	reducerInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	e := joinerOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T2_1)

			reducedData := &reduced.TotalProfitBySubtotal{}
			err := proto.Unmarshal(dataBatch.Payload, reducedData)

			assert.Nil(t, err)

			assert.Equal(t, ReducedTransactionMock2_1.GetItemId(), reducedData.GetItemId())
			assert.Equal(t, ReducedTransactionMock2_1.GetYearMonth(), reducedData.GetYearMonth())
			assert.Equal(t, ReducedTransactionMock2_1.GetSubtotal(), reducedData.GetSubtotal())

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
}

func reduceTask2_2(t *testing.T) {
	reducerInputQueue := middleware.GetReducerQueue(url)
	joinerOutputQueue := middleware.GetJoinerQueue(url)
	defer reducerInputQueue.StopConsuming()
	defer joinerOutputQueue.StopConsuming()
	defer reducerInputQueue.Close()
	defer joinerOutputQueue.Close()

	serializedTransactions, _ := proto.Marshal(&GroupTransactionMock2)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client",
		TaskType: int32(enum.T2_2),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	reducerInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	e := joinerOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T2_2)

			reducedData := &reduced.TotalSoldByQuantity{}
			err := proto.Unmarshal(dataBatch.Payload, reducedData)

			assert.Nil(t, err)

			assert.Equal(t, ReducedTransactionMock2_2.GetItemId(), reducedData.GetItemId())
			assert.Equal(t, ReducedTransactionMock2_2.GetYearMonth(), reducedData.GetYearMonth())
			assert.Equal(t, ReducedTransactionMock2_2.GetQuantity(), reducedData.GetQuantity())

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
}

func reduceTask4(t *testing.T) {
	reducerInputQueue := middleware.GetReducerQueue(url)
	joinerOutputQueue := middleware.GetJoinerQueue(url)
	defer reducerInputQueue.StopConsuming()
	defer joinerOutputQueue.StopConsuming()
	defer reducerInputQueue.Close()
	defer joinerOutputQueue.Close()

	serializedTransactions, _ := proto.Marshal(&GroupTransactionMock4)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client",
		TaskType: int32(enum.T4),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	reducerInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	e := joinerOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T4)

			reducedData := &reduced.CountedUserTransactions{}
			err := proto.Unmarshal(dataBatch.Payload, reducedData)

			assert.Nil(t, err)

			assert.Equal(t, ReducedTransactionMock4.GetUserId(), reducedData.GetUserId())
			assert.Equal(t, ReducedTransactionMock4.GetStoreId(), reducedData.GetStoreId())
			assert.Equal(t, ReducedTransactionMock4.GetTransactionQuantity(), reducedData.GetTransactionQuantity())

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
}
