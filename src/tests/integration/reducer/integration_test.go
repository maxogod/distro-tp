package reducer_test

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
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
	logger.InitLogger(logger.LoggerEnvDevelopment)
	m.Run()
}

// TestSequentialRun runs tests in sequence to
// avoid consuming conflicts on the same queues.
func TestSequentialRun(t *testing.T) {
	tests := []func(t *testing.T){
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
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	serializedTransactions, _ := proto.Marshal(&GroupTransactionMock1)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client-3",
		TaskType: int32(enum.T3),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	reducerInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	e := aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T3)

			reducedData := &reduced.TotalPaymentValueBatch{}
			err := proto.Unmarshal(dataBatch.Payload, reducedData)

			assert.Nil(t, err)

			for _, item := range reducedData.TotalPaymentValues {
				assert.Equal(t, ReducedTransactionMock1.GetFinalAmount(), item.GetFinalAmount())
				assert.Equal(t, ReducedTransactionMock1.GetStoreId(), item.GetStoreId())
				assert.Equal(t, ReducedTransactionMock1.GetSemester(), item.GetSemester())
			}
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

	aggregatorOutputQueue.StopConsuming()
	reducerInputQueue.Close()
	aggregatorOutputQueue.Close()
}

func reduceTask4(t *testing.T) {
	reducerInputQueue := middleware.GetReducerQueue(url)
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	serializedTransactions, _ := proto.Marshal(&GroupTransactionMock4)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client-4",
		TaskType: int32(enum.T4),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	reducerInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	e := aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			assert.True(t, enum.TaskType(dataBatch.TaskType) == enum.T4)

			reducedData := &reduced.CountedUserTransactionBatch{}
			err := proto.Unmarshal(dataBatch.Payload, reducedData)

			assert.Nil(t, err)

			for _, countedUserTransaction := range reducedData.CountedUserTransactions {
				assert.Equal(t, ReducedTransactionMock4.GetUserId(), countedUserTransaction.GetUserId())
				assert.Equal(t, ReducedTransactionMock4.GetStoreId(), countedUserTransaction.GetStoreId())
				assert.Equal(t, ReducedTransactionMock4.GetTransactionQuantity(), countedUserTransaction.GetTransactionQuantity())
			}
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

	aggregatorOutputQueue.StopConsuming()
	reducerInputQueue.Close()
	aggregatorOutputQueue.Close()
}
