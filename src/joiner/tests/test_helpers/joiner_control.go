package test_helpers

import (
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	joiner "github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

const (
	RabbitURL = "amqp://guest:guest@localhost:5672/"
)

func SendDoneMessage(t *testing.T, pub middleware.MessageMiddleware, datasetType models.TaskType) {
	doneMsg := &protocol.Done{
		TaskType: int32(datasetType),
	}

	msgProto := &protocol.ReferenceQueueMessage{
		Payload: &protocol.ReferenceQueueMessage_Done{
			Done: doneMsg,
		},
	}

	doneBytes, err := proto.Marshal(msgProto)
	assert.NoError(t, err)

	e := pub.Send(doneBytes)
	assert.Equal(t, 0, int(e))
}

func StartJoiner(t *testing.T, rabbitURL string, storeDir string, refQueueNames []string) *joiner.Joiner {
	t.Helper()

	joinerConfig := config.Config{
		GatewayAddress:              rabbitURL,
		StorePath:                   storeDir,
		StoreTPVQueue:               "store_tpv",
		TransactionCountedQueue:     "transaction_counted",
		TransactionSumQueue:         "transaction_sum",
		UserTransactionsQueue:       "user_transactions",
		JoinedTransactionsQueue:     "joined_transactions_queue",
		JoinedStoresTPVQueue:        "joined_stores_tpv_queue",
		JoinedUserTransactionsQueue: "joined_user_transactions_queue",
	}

	j := joiner.NewJoiner(&joinerConfig)

	for _, refQueueName := range refQueueNames {
		err := j.StartRefConsumer(refQueueName)
		assert.NoError(t, err)
	}

	return j
}

func SendDataBatch(t *testing.T, inputQueue string, dataBatch *protocol.DataBatch) {
	t.Helper()

	pubProcessedData, err := middleware.NewQueueMiddleware(RabbitURL, inputQueue)
	assert.NoError(t, err)
	defer func() {
		_ = pubProcessedData.Close()
	}()

	dataMessage, err := proto.Marshal(dataBatch)
	assert.NoError(t, err)
	e := pubProcessedData.Send(dataMessage)
	assert.Equal(t, 0, int(e))
}

func SendReferenceBatches(t *testing.T, pub middleware.MessageMiddleware, csvPayloads [][]byte, datasetType models.RefDatasetType) {
	t.Helper()

	refBatch, err := GetPayloadForDatasetType(t, datasetType, csvPayloads)
	assert.NoError(t, err)

	msgProto := &protocol.ReferenceQueueMessage{
		Payload: &protocol.ReferenceQueueMessage_ReferenceBatch{
			ReferenceBatch: refBatch,
		},
	}

	msgBytes, err := proto.Marshal(msgProto)
	assert.NoError(t, err)

	e := pub.Send(msgBytes)
	assert.Equal(t, 0, int(e))
}

func GetOutputMessage(t *testing.T, outputQueue string) *protocol.DataBatch {
	t.Helper()

	consumer, err := middleware.NewQueueMiddleware(RabbitURL, outputQueue)
	assert.NoError(t, err)
	defer consumer.Close()

	var received *protocol.DataBatch
	done := make(chan struct{})

	consumer.StartConsuming(func(ch middleware.ConsumeChannel, d chan error) {
		for msg := range ch {
			var batch protocol.DataBatch
			unmErr := proto.Unmarshal(msg.Body, &batch)
			assert.NoError(t, unmErr)

			received = &batch

			err = msg.Ack(false)
			assert.NoError(t, err)

			d <- nil
			close(done)
			return
		}
	})

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("did not receive batch from %s", outputQueue)
	}

	return received
}
