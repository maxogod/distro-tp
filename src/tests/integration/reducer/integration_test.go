package reducer_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/reducer/mock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	go mock.StartReducerMock("./config_test.yaml")
	m.Run()
}

func TestReduceTask3(t *testing.T) {

	url := "amqp://guest:guest@localhost:5672/"

	reducerInputQueue := middleware.GetReducerQueue(url)
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	defer reducerInputQueue.Close()
	defer aggregatorOutputQueue.Close()

	serializedTransactions, _ := proto.Marshal(&GroupTransactionMock1)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client",
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
	<-done
	assert.Equal(t, 0, int(e))
}

func TestReduceTask2_1(t *testing.T) {

	url := "amqp://guest:guest@localhost:5672/"

	reducerInputQueue := middleware.GetReducerQueue(url)
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	defer reducerInputQueue.Close()
	defer aggregatorOutputQueue.Close()

	serializedTransactions, _ := proto.Marshal(&GroupTransactionMock2)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client",
		TaskType: int32(enum.T2_1),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	reducerInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	e := aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
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
	<-done
	assert.Equal(t, 0, int(e))
}

func TestReduceTask2_2(t *testing.T) {

	url := "amqp://guest:guest@localhost:5672/"

	reducerInputQueue := middleware.GetReducerQueue(url)
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	defer reducerInputQueue.Close()
	defer aggregatorOutputQueue.Close()

	serializedTransactions, _ := proto.Marshal(&GroupTransactionMock2)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client",
		TaskType: int32(enum.T2_2),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	reducerInputQueue.Send(serializedDataEnvelope)

	done := make(chan bool, 1)

	e := aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
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
	<-done
	assert.Equal(t, 0, int(e))
}
