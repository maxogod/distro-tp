package group_by_test

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

	go func() {
		mock.StartReducerMock("./config_test.yaml")
	}()

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
