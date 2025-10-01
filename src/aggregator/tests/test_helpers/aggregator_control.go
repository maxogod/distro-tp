package test_helpers

import (
	"testing"
	"time"

	aggregator "github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func AggregatorConfig(storeDir string) config.Config {
	return config.Config{
		GatewayAddress:                     RabbitURL,
		StorePath:                          storeDir,
		JoinedMostProfitsTransactionsQueue: "joined_most_profits_transactions",
		JoinedBestSellingTransactionsQueue: "joined_best_selling_transactions",
		JoinedStoresTPVQueue:               "joined_stores_tpv",
		JoinedUserTransactionsQueue:        "joined_user_transactions",
		GatewayControllerDataQueue:         "processed_data",
		GatewayControllerConnectionQueue:   "node_connections",
		GatewayControllerExchange:          "finish_exchange",
		FinishRoutingKey:                   "aggregator",
	}
}

func StartAggregator(t *testing.T, storeDir string, dataQueueNames []string) *aggregator.Aggregator {
	t.Helper()

	aggregatorConfig := AggregatorConfig(storeDir)

	agg := aggregator.NewAggregator(&aggregatorConfig)

	for _, dataQueueName := range dataQueueNames {
		err := agg.StartDataConsumer(dataQueueName)
		assert.NoError(t, err)
	}

	return agg
}

func SendDataBatch(t *testing.T, dataQueue middleware.MessageMiddleware, dataBatch *data_batch.DataBatch) {
	t.Helper()

	dataMessage, err := proto.Marshal(dataBatch)
	assert.NoError(t, err)
	e := dataQueue.Send(dataMessage)
	assert.Equal(t, 0, int(e))
}

func SendDoneMessage(t *testing.T, aggregatorConfig config.Config) {
	t.Helper()

	finishExchange, err := middleware.NewExchangeMiddleware(
		RabbitURL,
		aggregatorConfig.GatewayControllerExchange,
		"direct",
		[]string{aggregatorConfig.FinishRoutingKey},
	)
	assert.NoError(t, err)

	finishMsg := &data_batch.DataBatch{
		TaskType: int32(enum.T4),
		Done:     true,
	}

	dataMessage, err := proto.Marshal(finishMsg)
	assert.NoError(t, err)
	e := finishExchange.Send(dataMessage)
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
			msgProto, unMarshalErr := unmarshal(msg.Body)
			assert.NoError(t, unMarshalErr)

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
