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
const datasetTypeUsers = 2

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

func TestJoinerPersistReferenceBatchesUsers(t *testing.T) {
	rabbitURL := "amqp://guest:guest@localhost:5672/"
	refQueueName := "test_users"
	storeDir := t.TempDir()
	datasetName := "users"

	j := helper.StartJoiner(t, rabbitURL, storeDir, refQueueName)
	defer j.Stop()

	pub, err := middleware.NewQueueMiddleware(rabbitURL, refQueueName)
	assert.NoError(t, err)
	defer func() {
		_ = pub.Delete()
		_ = pub.Close()
	}()

	csvPayloads := [][]byte{
		[]byte("1,female,1970-04-22,2023-07-01 08:13:07\n"),
		[]byte("581,male,2004-06-13,2023-08-01 09:39:30\n"),
	}
	helper.SendReferenceBatches(t, pub, csvPayloads, datasetTypeUsers)

	expectedFile := filepath.Join(storeDir, fmt.Sprintf("%s_202307.csv", datasetName))
	anotherExpectedFile := filepath.Join(storeDir, fmt.Sprintf("%s_202308.csv", datasetName))

	helper.AssertFileContainsPayloads(t, expectedFile, [][]byte{csvPayloads[0]})
	helper.AssertFileContainsPayloads(t, anotherExpectedFile, [][]byte{csvPayloads[1]})
}
