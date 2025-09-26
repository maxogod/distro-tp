package tests

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
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
