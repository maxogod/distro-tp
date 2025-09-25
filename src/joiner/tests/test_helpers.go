package tests

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	joiner "github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/protocol"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func SendReferenceBatches(t *testing.T, pub middleware.MessageMiddleware, csvPayloads [][]byte, datasetType int32) {
	t.Helper()

	for _, csvPayload := range csvPayloads {
		msg1Proto := &protocol.ReferenceBatch{
			DatasetType: datasetType,
			Payload:     csvPayload,
		}

		msg1Bytes, err := proto.Marshal(msg1Proto)
		assert.NoError(t, err)

		e := pub.Send(msg1Bytes)
		assert.Equal(t, 0, int(e))
	}
}

func StartJoiner(t *testing.T, rabbitURL string, storeDir string, refQueueName string) *joiner.Joiner {
	t.Helper()

	joinerConfig := config.Config{
		GatewayAddress: rabbitURL,
		StorePath:      storeDir,
	}

	j := joiner.NewJoiner(&joinerConfig)

	err := j.StartRefConsumer(refQueueName)
	assert.NoError(t, err)
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
