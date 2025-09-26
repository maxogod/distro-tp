package tests_test

import (
	"path/filepath"
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	helper "github.com/maxogod/distro-tp/src/joiner/tests"
	"github.com/stretchr/testify/assert"
)

const (
	DatasetMenuItems = 0
	DatasetStores    = 1
	DatasetUsers     = 2
	Task3            = 3
	Task4            = 4
)

func TestJoinerPersistReferenceBatchesMenuItems(t *testing.T) {
	storeDir := t.TempDir()
	testCase := helper.TestCase{
		Queue:       "test_menu_items",
		DatasetType: DatasetMenuItems,
		CsvPayloads: [][]byte{
			[]byte("1,Espresso,coffee,6.0,False,,\n"),
			[]byte("2,Americano,coffee,7.0,False,,\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "menu_items.csv")},
		TaskDone:      0,
		SendDone:      false,
	}
	helper.RunTest(t, storeDir, testCase)
}

func TestJoinerPersistReferenceBatchesUsers(t *testing.T) {
	storeDir := t.TempDir()
	testCase := helper.TestCase{
		Queue:       "test_users",
		DatasetType: DatasetUsers,
		CsvPayloads: [][]byte{
			[]byte("1,female,1970-04-22,2023-07-01 08:13:07\n"), // payload for expectedFiles[0]
			[]byte("581,male,2004-06-13,2023-08-01 09:39:30\n"), // payload for expectedFiles[1]
		},
		ExpectedFiles: []string{
			filepath.Join(storeDir, "users_202307.csv"),
			filepath.Join(storeDir, "users_202308.csv"),
		},
		TaskDone: Task4,
		SendDone: false,
	}
	helper.RunTest(t, storeDir, testCase)
}

func TestJoinerHandlesDoneAndConsumesNextQueueTask3(t *testing.T) {
	storeDir := t.TempDir()
	testCase := helper.TestCase{
		Queue:       "test_stores",
		DatasetType: DatasetStores,
		CsvPayloads: [][]byte{
			[]byte("1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
			[]byte("2,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "stores.csv")},
		TaskDone:      Task3,
		SendDone:      true,
	}
	helper.RunTest(t, storeDir, testCase)

	storesTPVQueue := "store_tpv"
	pubProcessedData, err := middleware.NewQueueMiddleware(helper.RabbitURL, storesTPVQueue)
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
	storeDir := t.TempDir()
	testCaseStores := helper.TestCase{
		Queue:       "test_stores",
		DatasetType: DatasetStores,
		CsvPayloads: [][]byte{
			[]byte("1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
			[]byte("2,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
			[]byte("3,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
			[]byte("4,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "stores.csv")},
		TaskDone:      Task4,
		SendDone:      true,
	}
	helper.RunTest(t, storeDir, testCaseStores)

	testCaseUsers := helper.TestCase{
		Queue:       "test_users",
		DatasetType: DatasetUsers,
		CsvPayloads: [][]byte{
			[]byte("1,female,1970-04-22,2023-07-01 08:13:07\n"),
			[]byte("2,female,1970-04-22,2023-07-01 08:13:07\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "users_202307.csv")},
		TaskDone:      Task4,
		SendDone:      true,
	}
	helper.RunTest(t, storeDir, testCaseUsers)
}
