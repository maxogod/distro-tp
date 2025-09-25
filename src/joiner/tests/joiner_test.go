package tests_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	helper "github.com/maxogod/distro-tp/src/joiner/tests"
	"github.com/stretchr/testify/assert"
)

const datasetTypeMenuItems = 0

func TestJoinerPersistReferenceBatchesMenuItems(t *testing.T) {
	rabbitURL := "amqp://guest:guest@localhost:5672/"
	refQueueName := "test_menu_items"
	storeDir := t.TempDir()
	datasetName := "menu_items"

	j := helper.StartJoiner(t, rabbitURL, storeDir, refQueueName)
	defer j.Stop()

	pub, err := middleware.NewQueueMiddleware(rabbitURL, refQueueName)
	assert.NoError(t, err)
	defer func() {
		_ = pub.Delete()
		_ = pub.Close()
	}()

	csvPayloads := [][]byte{
		[]byte("1,Espresso,coffee,6.0,False,,\n"),
		[]byte("2,Americano,coffee,7.0,False,,\n"),
	}
	helper.SendReferenceBatches(t, pub, csvPayloads, datasetTypeMenuItems)

	expectedFile := filepath.Join(storeDir, fmt.Sprintf("%s.csv", datasetName))

	helper.AssertFileContainsPayloads(t, expectedFile, csvPayloads)
}
