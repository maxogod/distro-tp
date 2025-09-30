package test_helpers

import (
	"sync"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	joiner "github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/internal/server"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

const (
	RabbitURL = "amqp://guest:guest@localhost:5672/"
)

func JoinerConfig(storeDir string) config.Config {
	return config.Config{
		GatewayAddress:              RabbitURL,
		StorePath:                   storeDir,
		StoreTPVQueue:               "store_tpv",
		TransactionCountedQueue:     "transaction_counted",
		TransactionSumQueue:         "transaction_sum",
		UserTransactionsQueue:       "user_transactions",
		JoinedTransactionsQueue:     "joined_transactions_queue",
		JoinedStoresTPVQueue:        "joined_stores_tpv_queue",
		JoinedUserTransactionsQueue: "joined_user_transactions_queue",
		GatewayControllerQueue:      "node_connections",
		GatewayControllerExchange:   "finish_exchange",
		FinishRoutingKey:            "joiner",
	}
}

func SendDoneMessage(t *testing.T, pub middleware.MessageMiddleware, datasetType enum.TaskType) {
	doneMsg := &data_batch.DataBatch{
		TaskType: int32(datasetType),
		Done:     true,
	}

	doneBytes, err := proto.Marshal(doneMsg)
	assert.NoError(t, err)

	e := pub.Send(doneBytes)
	assert.Equal(t, 0, int(e))
}

func StartJoiner(t *testing.T, storeDir string, refQueueNames []string) *joiner.Joiner {
	t.Helper()

	joinerConfig := JoinerConfig(storeDir)

	j := joiner.NewJoiner(&joinerConfig)

	for _, refQueueName := range refQueueNames {
		err := j.StartRefConsumer(refQueueName)
		assert.NoError(t, err)
	}

	return j
}

func SendDataBatch(t *testing.T, inputQueue string, dataBatch *data_batch.DataBatch) {
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

func SendReferenceBatches(t *testing.T, pub middleware.MessageMiddleware, csvPayloads [][]byte, datasetType enum.RefDatasetType, taskType enum.TaskType) {
	t.Helper()

	refBatch, err := GetPayloadForDatasetType(t, datasetType, taskType, csvPayloads)
	assert.NoError(t, err)

	msgBytes, err := proto.Marshal(refBatch)
	assert.NoError(t, err)

	e := pub.Send(msgBytes)
	assert.Equal(t, 0, int(e))
}

func GetAllOutputMessages[T proto.Message](
	t *testing.T,
	outputQueue string,
	unmarshal func([]byte) (T, error),
) []T {
	t.Helper()

	consumer, err := middleware.NewQueueMiddleware(RabbitURL, outputQueue)
	assert.NoError(t, err)
	defer func() {
		_ = consumer.StopConsuming()
		_ = consumer.Close()
	}()

	var received []T
	done := make(chan struct{})

	consumer.StartConsuming(func(ch middleware.ConsumeChannel, d chan error) {
		for msg := range ch {
			msgProto, unMarshalerr := unmarshal(msg.Body)
			assert.NoError(t, unMarshalerr)

			received = append(received, msgProto)

			err = msg.Ack(false)
			assert.NoError(t, err)
		}
		close(done)
		d <- nil
	})

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		if len(received) > 0 {
			return received
		}
		t.Fatal("Timeout waiting for messages")
	}

	return received
}

func PayloadAsBestSelling(payload []byte) bool {
	var batch joined.JoinBestSellingProductsBatch

	err := proto.Unmarshal(payload, &batch)
	if err != nil {
		return false
	}

	for _, item := range batch.Items {
		mf := item.ProtoReflect()
		unknown := mf.GetUnknown()
		if len(unknown) > 0 {
			return false
		}
	}

	return true
}

func PayloadAsMostProfits(payload []byte) bool {
	var batch joined.JoinMostProfitsProductsBatch

	err := proto.Unmarshal(payload, &batch)
	if err != nil {
		return false
	}

	for _, item := range batch.Items {
		mf := item.ProtoReflect()
		unknown := mf.GetUnknown()
		if len(unknown) > 0 {
			return false
		}
	}

	return true
}

func InitServer(t *testing.T, storeDir string, wg *sync.WaitGroup) *server.Server {
	joinerConfig := JoinerConfig(storeDir)

	joinServer := server.InitServer(&joinerConfig)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := joinServer.Run()
		if err != nil {
			assert.NoError(t, err)
		}
	}()

	return joinServer
}
