package tests

import (
	"bytes"
	"os"
	"strconv"
	"strings"
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
	DatasetUsers = 2
	RabbitURL    = "amqp://guest:guest@localhost:5672/"
)

type TestCase struct {
	Queue         string
	DatasetType   int32
	CsvPayloads   [][]byte
	ExpectedFiles []string
	TaskDone      int32
	SendDone      bool
}

func RunTest(t *testing.T, storeDir string, c TestCase) {
	t.Helper()

	j := StartJoiner(t, RabbitURL, storeDir, []string{c.Queue})
	defer j.Stop()

	pub, err := middleware.NewQueueMiddleware(RabbitURL, c.Queue)
	assert.NoError(t, err)
	defer func() {
		_ = pub.Delete()
		_ = pub.Close()
	}()

	SendReferenceBatches(t, pub, c.CsvPayloads, c.DatasetType)

	for i, expectedFile := range c.ExpectedFiles {
		if c.DatasetType != DatasetUsers {
			AssertFileContainsPayloads(t, expectedFile, c.CsvPayloads, c.DatasetType)
		} else {
			AssertFileContainsPayloads(t, expectedFile, [][]byte{c.CsvPayloads[i]}, c.DatasetType)
		}
	}

	if c.SendDone {
		SendDoneMessage(t, pub, c.TaskDone)
	}
}

func SendReferenceBatches(t *testing.T, pub middleware.MessageMiddleware, csvPayloads [][]byte, datasetType int32) {
	t.Helper()

	for _, csvPayload := range csvPayloads {
		payload := csvPayload
		var err error

		if models.RefDatasetType(datasetType) == models.Users {
			payload, err = marshalPayloadForUsersDataset(t, csvPayload)
			assert.NoError(t, err)
		}

		refBatch := &protocol.ReferenceBatch{
			DatasetType: datasetType,
			Payload:     payload,
		}

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
}

func marshalPayloadForUsersDataset(t *testing.T, csvPayload []byte) ([]byte, error) {
	row := string(csvPayload)
	cols := strings.Split(strings.TrimSpace(row), ",")
	if len(cols) < 4 {
		t.Fatalf("invalid csv payload: %s", row)
	}

	userID, _ := strconv.Atoi(cols[0])
	birthdate := cols[2]
	registeredAt := cols[3]

	user := &protocol.User{
		UserId:       int32(userID),
		Gender:       cols[1],
		Birthdate:    birthdate,
		RegisteredAt: registeredAt,
	}

	return proto.Marshal(user)
}

func SendDoneMessage(t *testing.T, pub middleware.MessageMiddleware, datasetType int32) {
	doneMsg := &protocol.Done{
		TaskType: datasetType,
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

func AssertFileContainsPayloads(t *testing.T, expectedFile string, csvPayloads [][]byte, datasetType int32) {
	t.Helper()

	timeout := time.After(6 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	var expectedPayloads [][]byte
	for _, csvPayload := range csvPayloads {
		if models.RefDatasetType(datasetType) == models.Users {
			userPayload, err := marshalPayloadForUsersDataset(t, csvPayload)
			assert.NoError(t, err)
			expectedPayloads = append(expectedPayloads, userPayload)
		} else {
			expectedPayloads = append(expectedPayloads, csvPayload)
		}
	}

	var data []byte
	found := false
	for !found {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for file %s", expectedFile)
		case <-tick.C:
			if _, fileErr := os.Stat(expectedFile); fileErr == nil {
				data, fileErr = os.ReadFile(expectedFile)
				if fileErr != nil {
					t.Fatalf("read error: %v", fileErr)
				}

				allPresent := true
				for _, payload := range expectedPayloads {
					if !bytes.Contains(data, payload) {
						allPresent = false
						break
					}
				}
				if allPresent {
					found = true
				}
			}
		}
	}

	if !found {
		t.Fatalf("expected payloads not present in file: %s", expectedFile)
	}
}

func AssertJoinerConsumed(t *testing.T, m middleware.MessageMiddleware, expected string) {
	t.Helper()

	time.Sleep(200 * time.Millisecond)

	found := false
	err := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			if string(msg.Body) == expected {
				found = true
			}
			d <- nil
			break
		}
	})
	assert.Equal(t, 0, int(err))

	_ = m.StopConsuming()

	if found {
		t.Fatalf("message '%s' was not consumed by the joiner", expected)
	}
}

func PrepareDataBatch[T any](
	t *testing.T,
	taskType int32,
	items []*T,
	createContainer func([]*T) proto.Message,
) *protocol.DataBatch {
	t.Helper()

	container := createContainer(items)

	payload, err := proto.Marshal(container)
	assert.NoError(t, err)

	return &protocol.DataBatch{
		TaskType: taskType,
		Payload:  payload,
	}
}

func PrepareStoreTPVBatch(t *testing.T, tpvs []*protocol.StoreTPV) *protocol.DataBatch {
	return PrepareDataBatch(t, 3, tpvs, func(items []*protocol.StoreTPV) proto.Message {
		return &protocol.StoresTPV{Items: items}
	})
}

func PrepareBestSellingBatch(t *testing.T, records []*protocol.BestSellingProducts) *protocol.DataBatch {
	return PrepareDataBatch(t, 4, records, func(items []*protocol.BestSellingProducts) proto.Message {
		return &protocol.BestSellingProductsBatch{Items: items}
	})
}

func PrepareMostProfitsBatch(t *testing.T, records []*protocol.MostProfitsProducts) *protocol.DataBatch {
	return PrepareDataBatch(t, 4, records, func(items []*protocol.MostProfitsProducts) proto.Message {
		return &protocol.MostProfitsProductsBatch{Items: items}
	})
}

func SendDataBatch(t *testing.T, inputQueue string, dataBatch *protocol.DataBatch) {
	t.Helper()

	pubProcessedData, err := middleware.NewQueueMiddleware(RabbitURL, inputQueue)
	assert.NoError(t, err)
	defer func() {
		_ = pubProcessedData.Delete()
		_ = pubProcessedData.Close()
	}()

	dataMessage, err := proto.Marshal(dataBatch)
	assert.NoError(t, err)
	e := pubProcessedData.Send(dataMessage)
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
			d <- nil
			close(done)
			return
		}
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive batch from aggregator")
	}

	return received
}

func AssertJoinedBatchIsTheExpected[T any, B proto.Message](
	t *testing.T,
	received *protocol.DataBatch,
	expected []*T,
	unmarshalBatch func([]byte) (B, []*T, error),
	compare func(exp, got *T, idx int, t *testing.T),
) {
	t.Helper()

	assert.NotNil(t, received, "received DataBatch should not be nil")

	// Deserialize into the right batch type
	_, items, err := unmarshalBatch(received.Payload)
	assert.NoError(t, err, "failed to unmarshal DataBatch.Payload")

	assert.Len(t, items, len(expected), "unexpected number of joined records")

	for i, exp := range expected {
		if i >= len(items) {
			t.Fatalf("expected at least %d items but got %d", len(expected), len(items))
		}
		got := items[i]
		compare(exp, got, i, t)
	}
}

func AssertJoinedStoreTPVIsExpected(
	t *testing.T,
	received *protocol.DataBatch,
	expected []*protocol.JoinStoreTPV,
) {
	unmarshal := func(payload []byte) (*protocol.JoinStoreTPVBatch, []*protocol.JoinStoreTPV, error) {
		var batch protocol.JoinStoreTPVBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, nil, err
		}
		return &batch, batch.Items, nil
	}

	compare := func(exp, got *protocol.JoinStoreTPV, idx int, t *testing.T) {
		assert.Equal(t, exp.YearHalfCreatedAt, got.YearHalfCreatedAt, "YearHalfCreatedAt mismatch at index %d", idx)
		assert.Equal(t, exp.StoreName, got.StoreName, "StoreName mismatch at index %d", idx)
		assert.Equal(t, exp.Tpv, got.Tpv, "TPV mismatch at index %d", idx)
	}

	AssertJoinedBatchIsTheExpected(t, received, expected, unmarshal, compare)
}

func AssertJoinedBestSellingIsExpected(
	t *testing.T,
	received *protocol.DataBatch,
	expected []*protocol.JoinBestSellingProducts,
) {
	unmarshal := func(payload []byte) (*protocol.JoinBestSellingProductsBatch, []*protocol.JoinBestSellingProducts, error) {
		var batch protocol.JoinBestSellingProductsBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, nil, err
		}
		return &batch, batch.Items, nil
	}

	compare := func(exp, got *protocol.JoinBestSellingProducts, idx int, t *testing.T) {
		assert.Equal(t, exp.YearMonthCreatedAt, got.YearMonthCreatedAt, "YearMonthCreatedAt mismatch at index %d", idx)
		assert.Equal(t, exp.ItemName, got.ItemName, "ItemName mismatch at index %d", idx)
		assert.Equal(t, exp.SellingsQty, got.SellingsQty, "SellingsQty mismatch at index %d", idx)
	}

	AssertJoinedBatchIsTheExpected(t, received, expected, unmarshal, compare)
}

func AssertJoinedMostProfitsIsExpected(
	t *testing.T,
	received *protocol.DataBatch,
	expected []*protocol.JoinMostProfitsProducts,
) {
	unmarshal := func(payload []byte) (*protocol.JoinMostProfitsProductsBatch, []*protocol.JoinMostProfitsProducts, error) {
		var batch protocol.JoinMostProfitsProductsBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, nil, err
		}
		return &batch, batch.Items, nil
	}

	compare := func(exp, got *protocol.JoinMostProfitsProducts, idx int, t *testing.T) {
		assert.Equal(t, exp.YearMonthCreatedAt, got.YearMonthCreatedAt, "YearMonthCreatedAt mismatch at index %d", idx)
		assert.Equal(t, exp.ItemName, got.ItemName, "ItemName mismatch at index %d", idx)
		assert.Equal(t, exp.ProfitSum, got.ProfitSum, "ProfitSum mismatch at index %d", idx)
	}

	AssertJoinedBatchIsTheExpected(t, received, expected, unmarshal, compare)
}
