package integration_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/filter/config"
	"github.com/maxogod/distro-tp/src/filter/internal/server"
	"github.com/maxogod/distro-tp/src/filter/test/integration"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func startFilterMock() {
	conf, err := config.InitConfig("./config_test.yaml")
	if err != nil {
		panic(err)
	}
	server := server.InitServer(conf)
	server.Run()
}

func TestFilterIO(t *testing.T) {

	url := "amqp://guest:guest@localhost:5672/"

	filterQueue := middleware.GetFilterQueue(url)
	aggregatorOutputQueue := middleware.GetAggregatorQueue(url)

	defer filterQueue.Close()
	defer aggregatorOutputQueue.Close()

	serializedTransactions, _ := proto.Marshal(&integration.MockTransactionsBatch)

	dataEnvelope := protocol.DataEnvelope{
		ClientId: "test-client",
		TaskType: int32(enum.T1),
		Payload:  serializedTransactions,
	}

	serializedDataEnvelope, _ := proto.Marshal(&dataEnvelope)

	filterQueue.Send(serializedDataEnvelope)

	go func() {
		startFilterMock()
	}()

	done := make(chan bool, 1)

	e := aggregatorOutputQueue.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			msg.Ack(false)
			dataBatch, _ := utils.GetDataEnvelope(msg.Body)
			TransactionData := &raw.TransactionBatch{}
			err := proto.Unmarshal(dataBatch.Payload, TransactionData)
			assert.Nil(t, err)
			assert.Equal(t, len(integration.MockTransactionsBatch.Transactions), len(TransactionData.Transactions))
			for i, sentTx := range integration.MockTransactionsBatch.Transactions {
				gotTx := TransactionData.Transactions[i]
				assert.True(t, proto.Equal(sentTx, gotTx), "Transaction at index %d does not match", i)
			}
			break
		}
		done <- true
	})
	<-done
	assert.Equal(t, 0, int(e))
}
