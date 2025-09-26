package tests

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/protocol"
	joiner "github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/config"
	jProtocol "github.com/maxogod/distro-tp/src/joiner/protocol"
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
			AssertFileContainsPayloads(t, expectedFile, c.CsvPayloads)
		} else {
			AssertFileContainsPayloads(t, expectedFile, [][]byte{c.CsvPayloads[i]})
		}
	}

	if c.SendDone {
		SendDoneMessage(t, pub, c.TaskDone)
	}
}

func SendReferenceBatches(t *testing.T, pub middleware.MessageMiddleware, csvPayloads [][]byte, datasetType int32) {
	t.Helper()

	for _, csvPayload := range csvPayloads {
		refBatch := &protocol.ReferenceBatch{
			DatasetType: datasetType,
			Payload:     csvPayload,
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
		GatewayAddress:          rabbitURL,
		StorePath:               storeDir,
		StoreTPVQueue:           "store_tpv",
		TransactionCountedQueue: "transaction_counted",
		TransactionSumQueue:     "transaction_sum",
		UserTransactionsQueue:   "user_transactions",
		AggregatorQueue:         "aggregator",
	}

	j := joiner.NewJoiner(&joinerConfig)

	for _, refQueueName := range refQueueNames {
		err := j.StartRefConsumer(refQueueName)
		assert.NoError(t, err)
	}

	return j
}

func AssertFileContainsPayloads(t *testing.T, expectedFile string, csvPayloads [][]byte) {
	t.Helper()

	timeout := time.After(6 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

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
				for _, payload := range csvPayloads {
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
		t.Fatalf("expected payloads not present in file: %s\ncontent: %s", expectedFile, string(data))
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

func PrepareStoresCSV(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	storesCSV := filepath.Join(dir, "stores.csv")

	storesContent := `store_id,store_name,street,postal_code,city,state,latitude,longitude
	5,G Coffee @ Seksyen 21,Jalan 1,12345,CityA,StateA,1.0,2.0
	6,G Coffee @ Alam Tun Hussein Onn,Jalan 2,23456,CityB,StateB,3.0,4.0
	4,G Coffee @ Kampung Changkat,Jalan 3,34567,CityC,StateC,5.0,6.0
	`

	if err := os.WriteFile(storesCSV, []byte(storesContent), 0644); err != nil {
		t.Fatalf("failed to write stores.csv: %v", err)
	}
}

func PrepareDataBatch(t *testing.T, tpvs []*jProtocol.StoreTPV) *protocol.DataBatch {
	var payload []byte
	for _, tpv := range tpvs {
		b, err := proto.Marshal(tpv)
		assert.NoError(t, err)
		payload = append(payload, b...)
	}

	return &protocol.DataBatch{
		TaskType: 3,
		Payload:  payload,
	}
}

func SendDataBatch(t *testing.T, inputQueue string, dataBatch *protocol.DataBatch) {
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

func AssertJoinedBatchIsTheExpected(t *testing.T, received *protocol.DataBatch, expected []*jProtocol.JoinStoreTPV) {
	assert.NotNil(t, received, "received DataBatch should not be nil")

	var joinedBatch jProtocol.JoinStoreTPVBatch
	err := proto.Unmarshal(received.Payload, &joinedBatch)
	assert.NoError(t, err, "failed to unmarshal DataBatch.Payload into JoinStoreTPVBatch")

	assert.Len(t, joinedBatch.Items, len(expected), "unexpected number of joined records")

	for i, exp := range expected {
		if i >= len(joinedBatch.Items) {
			t.Fatalf("expected at least %d items but got %d", len(expected), len(joinedBatch.Items))
		}
		got := joinedBatch.Items[i]

		assert.Equal(t, exp.YearHalfCreatedAt, got.YearHalfCreatedAt, "YearHalfCreatedAt mismatch at index %d", i)
		assert.Equal(t, exp.StoreName, got.StoreName, "StoreName mismatch at index %d", i)
		assert.Equal(t, exp.Tpv, got.Tpv, "TPV mismatch at index %d", i)
	}
}
