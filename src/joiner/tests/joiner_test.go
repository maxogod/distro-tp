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
const datasetTypeStores = 1
const datasetTypeUsers = 2

func TestJoinerPersistReferenceBatchesMenuItems(t *testing.T) {
	rabbitURL := "amqp://guest:guest@localhost:5672/"
	refQueueName := "test_menu_items"
	storeDir := t.TempDir()
	datasetName := "menu_items"

	j := helper.StartJoiner(t, rabbitURL, storeDir, []string{refQueueName}, "")
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

	j := helper.StartJoiner(t, rabbitURL, storeDir, []string{refQueueName}, "")
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

func TestJoinerHandlesDoneAndConsumesNextQueueTask3(t *testing.T) {
	rabbitURL := "amqp://guest:guest@localhost:5672/"
	refQueueName := "test_menu_items"
	storesTPVQueue := "test_stores_TPV_queue"
	storeDir := t.TempDir()

	j := helper.StartJoiner(t, rabbitURL, storeDir, []string{refQueueName}, storesTPVQueue)
	defer j.Stop()

	pubRef, err := middleware.NewQueueMiddleware(rabbitURL, refQueueName)
	assert.NoError(t, err)
	defer func() {
		_ = pubRef.Delete()
		_ = pubRef.Close()
	}()

	csvPayloads := [][]byte{
		[]byte("1,Espresso,coffee,6.0,False,,\n"),
		[]byte("2,Americano,coffee,7.0,False,,\n"),
	}
	helper.SendReferenceBatches(t, pubRef, csvPayloads, datasetTypeMenuItems)

	helper.SendDoneMessage(t, pubRef)

	pubProcessedData, err := middleware.NewQueueMiddleware(rabbitURL, storesTPVQueue)
	assert.NoError(t, err)
	defer func() {
		_ = pubProcessedData.Delete()
		_ = pubProcessedData.Close()
	}()

	dataMessage := "Data Message"
	e := pubProcessedData.Send([]byte(dataMessage))
	assert.Equal(t, 0, int(e))

	helper.AssertJoinerConsumed(t, pubProcessedData, dataMessage)
}

func TestJoinerPersistReferenceBatchesUsersAndStores(t *testing.T) {
	rabbitURL := "amqp://guest:guest@localhost:5672/"
	storeDir := t.TempDir()
	refQueueNames := []string{"test_users", "test_stores"}

	usersDatasetName := "users"
	storesDatasetName := "stores"

	j := helper.StartJoiner(t, rabbitURL, storeDir, refQueueNames, "")
	defer j.Stop()

	usersPub, queueErr := middleware.NewQueueMiddleware(rabbitURL, refQueueNames[0])
	assert.NoError(t, queueErr)
	defer func() {
		_ = usersPub.Delete()
		_ = usersPub.Close()
	}()

	storesPub, queueErr := middleware.NewQueueMiddleware(rabbitURL, refQueueNames[1])
	assert.NoError(t, queueErr)
	defer func() {
		_ = storesPub.Delete()
		_ = storesPub.Close()
	}()

	usersCsvPayloads := [][]byte{
		[]byte("1,female,1970-04-22,2023-07-01 08:13:07\n"),
		[]byte("2,female,1970-04-22,2023-07-01 08:13:07\n"),
	}
	helper.SendReferenceBatches(t, usersPub, usersCsvPayloads, datasetTypeUsers)
	helper.SendDoneMessage(t, usersPub)

	storesCsvPayloads := [][]byte{
		[]byte("1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
		[]byte("2,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
		[]byte("3,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
		[]byte("4,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
	}
	helper.SendReferenceBatches(t, storesPub, storesCsvPayloads, datasetTypeStores)
	helper.SendDoneMessage(t, storesPub)

	expectedFile := filepath.Join(storeDir, fmt.Sprintf("%s_202307.csv", usersDatasetName))
	anotherExpectedFile := filepath.Join(storeDir, fmt.Sprintf("%s.csv", storesDatasetName))

	helper.AssertFileContainsPayloads(t, expectedFile, usersCsvPayloads)
	helper.AssertFileContainsPayloads(t, anotherExpectedFile, storesCsvPayloads)
}
