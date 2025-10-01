package tests_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	joiner "github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/tests/test_helpers"
	helpers "github.com/maxogod/distro-tp/src/joiner/tests/test_helpers"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestJoinerPersistReferenceBatchesMenuItems(t *testing.T) {
	storeDir := t.TempDir()
	testCase := test_helpers.TestCase{
		QueueName:   "menu_items",
		Queue:       middleware.GetMenuItemsQueue,
		DatasetType: enum.MenuItems,
		CsvPayloads: [][]byte{
			[]byte("1,Espresso,coffee,6.0,False,,\n"),
			[]byte("2,Americano,coffee,7.0,False,,\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "menu_items.pb")},
		TaskDone:      enum.T2,
		SendDone:      false,
	}

	j := helpers.StartJoiner(t, storeDir, []string{testCase.QueueName})
	defer func(j *joiner.Joiner) {
		err := j.Stop()
		assert.NoError(t, err)
	}(j)

	test_helpers.RunTest(t, testCase)
}

func TestJoinerPersistReferenceBatchesUsers(t *testing.T) {
	storeDir := t.TempDir()
	testCase := test_helpers.TestCase{
		QueueName:   "users",
		Queue:       middleware.GetUsersQueue,
		DatasetType: enum.Users,
		CsvPayloads: [][]byte{
			[]byte("1,female,1970-04-22,2023-07-01 08:13:07\n"),
			[]byte("581,male,2004-06-13,2023-08-01 09:39:30\n"),
		},
		ExpectedFiles: []string{
			filepath.Join(storeDir, "users_1-581.pb"),
		},
		TaskDone: enum.T4,
		SendDone: false,
	}

	j := helpers.StartJoiner(t, storeDir, []string{testCase.QueueName})
	defer func(j *joiner.Joiner) {
		err := j.Stop()
		assert.NoError(t, err)
	}(j)

	test_helpers.RunTest(t, testCase)
}

func TestJoinerHandlesDoneAndConsumesNextQueueTask3(t *testing.T) {
	storeDir := t.TempDir()
	testCase := test_helpers.TestCase{
		QueueName:   "stores",
		Queue:       middleware.GetStoresQueue,
		DatasetType: enum.Stores,
		CsvPayloads: [][]byte{
			[]byte("1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
			[]byte("2,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "stores.pb")},
		TaskDone:      enum.T3,
		SendDone:      true,
	}

	j := helpers.StartJoiner(t, storeDir, []string{testCase.QueueName})
	defer func(j *joiner.Joiner) {
		err := j.Stop()
		assert.NoError(t, err)
	}(j)

	test_helpers.RunTest(t, testCase)

	storesTPVQueue := "store_tpv"
	pubProcessedData, err := middleware.NewQueueMiddleware(test_helpers.RabbitURL, storesTPVQueue)
	assert.NoError(t, err)
	defer func() {
		_ = pubProcessedData.Delete()
		_ = pubProcessedData.Close()
	}()

	dataMessage := "Data Message"
	e := pubProcessedData.Send([]byte(dataMessage))
	assert.Equal(t, 0, int(e))

	helpers.AssertJoinerConsumed(t, pubProcessedData, dataMessage)
}

func TestJoinerPersistReferenceBatchesUsersAndStores(t *testing.T) {
	storeDir := t.TempDir()
	testCaseStores := test_helpers.TestCase{
		QueueName:   "stores",
		Queue:       middleware.GetStoresQueue,
		DatasetType: enum.Stores,
		CsvPayloads: [][]byte{
			[]byte("1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
			[]byte("2,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
			[]byte("3,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
			[]byte("4,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "stores.pb")},
		TaskDone:      enum.T4,
		SendDone:      true,
	}

	testCaseUsers := test_helpers.TestCase{
		QueueName:   "users",
		Queue:       middleware.GetUsersQueue,
		DatasetType: enum.Users,
		CsvPayloads: [][]byte{
			[]byte("1,female,1970-04-22,2023-07-01 08:13:07\n"),
			[]byte("2,female,1970-04-22,2023-08-01 08:13:07\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "users_1-2.pb")},
		TaskDone:      enum.T4,
		SendDone:      true,
	}

	j := helpers.StartJoiner(t, storeDir, []string{testCaseStores.QueueName, testCaseUsers.QueueName})
	defer func(j *joiner.Joiner) {
		err := j.Stop()
		assert.NoError(t, err)
	}(j)

	test_helpers.RunTest(t, testCaseStores)
	test_helpers.RunTest(t, testCaseUsers)
}

func TestHandleTaskType3_ProducesJoinedBatch(t *testing.T) {
	storeDir := t.TempDir()
	testCase := test_helpers.TestCase{
		QueueName:   "stores",
		Queue:       middleware.GetStoresQueue,
		DatasetType: enum.Stores,
		CsvPayloads: [][]byte{
			[]byte("5,G Coffee @ Seksyen 21,Jalan 1,12345,CityA,StateA,1.0,2.0\n"),
			[]byte("6,G Coffee @ Alam Tun Hussein Onn,Jalan 2,23456,CityB,StateB,3.0,4.0\n"),
			[]byte("4,G Coffee @ Kampung Changkat,Jalan 3,34567,CityC,StateC,5.0,6.0\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "stores.pb")},
		TaskDone:      enum.T3,
		SendDone:      true,
	}

	j := helpers.StartJoiner(t, storeDir, []string{testCase.QueueName})
	defer func(j *joiner.Joiner) {
		err := j.Stop()
		assert.NoError(t, err)
	}(j)

	test_helpers.RunTest(t, testCase)

	tpvs := []*reduced.StoreTPV{
		{YearHalfCreatedAt: "2024-H1", StoreId: 5, Tpv: 12102556},
		{YearHalfCreatedAt: "2024-H2", StoreId: 6, Tpv: 12201348},
		{YearHalfCreatedAt: "2025-H1", StoreId: 4, Tpv: 12067810},
	}
	dataBatch := helpers.PrepareStoreTPVBatch(t, tpvs, enum.T3)

	helpers.SendDataBatch(t, middleware.GetStoresTPVQueue, dataBatch)

	received := helpers.GetAllOutputMessages(t, middleware.GetJoinedStoresTPVQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})[0]

	expectedTpvs := []*joined.JoinStoreTPV{
		{YearHalfCreatedAt: "2024-H1", StoreName: "G Coffee @ Seksyen 21", Tpv: 12102556},
		{YearHalfCreatedAt: "2024-H2", StoreName: "G Coffee @ Alam Tun Hussein Onn", Tpv: 12201348},
		{YearHalfCreatedAt: "2025-H1", StoreName: "G Coffee @ Kampung Changkat", Tpv: 12067810},
	}

	helpers.AssertJoinedStoreTPVIsExpected(t, received, expectedTpvs)
}

func TestHandleTaskType2_ProducesJoinedBatch(t *testing.T) {
	storeDir := t.TempDir()
	testCase := test_helpers.TestCase{
		QueueName:   "menu_items",
		Queue:       middleware.GetMenuItemsQueue,
		DatasetType: enum.MenuItems,
		CsvPayloads: [][]byte{
			[]byte("1,Espresso,coffee,6.0,False,,\n"),
			[]byte("2,Americano,coffee,7.0,False,,\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "menu_items.pb")},
		TaskDone:      enum.T2,
		SendDone:      true,
	}

	j := helpers.StartJoiner(t, storeDir, []string{testCase.QueueName})
	defer func(j *joiner.Joiner) {
		err := j.Stop()
		assert.NoError(t, err)
	}(j)

	test_helpers.RunTest(t, testCase)

	bestSelling := []*reduced.BestSellingProducts{
		{YearMonthCreatedAt: "2024-01", ItemId: 1, SellingsQty: 260611},
		{YearMonthCreatedAt: "2024-02", ItemId: 2, SellingsQty: 91218},
	}
	bestSellingBatch := helpers.PrepareBestSellingBatch(t, bestSelling, enum.T2)
	helpers.SendDataBatch(t, middleware.GetBestSellingTransactionsQueue, bestSellingBatch)

	mostProfits := []*reduced.MostProfitsProducts{
		{YearMonthCreatedAt: "2024-01", ItemId: 1, ProfitSum: 260611.0},
		{YearMonthCreatedAt: "2024-02", ItemId: 2, ProfitSum: 91218.0},
	}
	mostProfitsBatch := helpers.PrepareMostProfitsBatch(t, mostProfits, enum.T2)
	helpers.SendDataBatch(t, middleware.GetMostProfitsTransactionsQueue, mostProfitsBatch)

	expectedBestSelling := []*joined.JoinBestSellingProducts{
		{YearMonthCreatedAt: "2024-01", ItemName: "Espresso", SellingsQty: 260611},
		{YearMonthCreatedAt: "2024-02", ItemName: "Americano", SellingsQty: 91218},
	}

	expectedMostProfits := []*joined.JoinMostProfitsProducts{
		{YearMonthCreatedAt: "2024-01", ItemName: "Espresso", ProfitSum: 260611.0},
		{YearMonthCreatedAt: "2024-02", ItemName: "Americano", ProfitSum: 91218.0},
	}

	mostProfitsBatches := helpers.GetAllOutputMessages(t, middleware.GetJoinedMostProfitsTransactionsQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})

	bestSellingBatches := helpers.GetAllOutputMessages(t, middleware.GetJoinedBestSellingTransactionsQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})

	allBatches := append(mostProfitsBatches, bestSellingBatches...)

	var bestSellingJoined, mostProfitsJoined *data_batch.DataBatch
	for _, batch := range allBatches {
		if helpers.PayloadAsBestSelling(batch.Payload) {
			bestSellingJoined = batch
		} else if helpers.PayloadAsMostProfits(batch.Payload) {
			mostProfitsJoined = batch
		}
	}

	helpers.AssertJoinedBestSellingIsExpected(t, bestSellingJoined, expectedBestSelling)
	helpers.AssertJoinedMostProfitsIsExpected(t, mostProfitsJoined, expectedMostProfits)
}

func TestHandleTaskType4_ProducesJoinedBatch(t *testing.T) {
	storeDir := t.TempDir()
	testCase := test_helpers.TestCase{
		QueueName:   "stores",
		Queue:       middleware.GetStoresQueue,
		DatasetType: enum.Stores,
		CsvPayloads: [][]byte{
			[]byte("5,G Coffee @ Seksyen 21,Jalan 1,12345,CityA,StateA,1.0,2.0\n"),
			[]byte("6,G Coffee @ Alam Tun Hussein Onn,Jalan 2,23456,CityB,StateB,3.0,4.0\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "stores.pb")},
		TaskDone:      enum.T4,
		SendDone:      true,
	}

	testCaseUsers := test_helpers.TestCase{
		QueueName:   "users",
		Queue:       middleware.GetUsersQueue,
		DatasetType: enum.Users,
		CsvPayloads: [][]byte{
			[]byte("1,female,1970-04-22,2023-07-01 08:13:07\n"),
			[]byte("2,female,1974-06-21,2023-08-01 08:13:07\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "users_1-2.pb")},
		TaskDone:      enum.T4,
		SendDone:      true,
	}

	j := helpers.StartJoiner(t, storeDir, []string{testCase.QueueName, testCaseUsers.QueueName})
	defer func(j *joiner.Joiner) {
		err := j.Stop()
		assert.NoError(t, err)
	}(j)

	test_helpers.RunTest(t, testCase)
	test_helpers.RunTest(t, testCaseUsers)

	mostPurchasesUsers := []*reduced.MostPurchasesUser{
		{StoreId: 5, UserId: 1, PurchasesQty: 260611},
		{StoreId: 6, UserId: 2, PurchasesQty: 91218},
	}
	mostPurchasesUsersBatch := helpers.PrepareMostPurchasesUserBatch(t, mostPurchasesUsers, enum.T4)
	helpers.SendDataBatch(t, middleware.GetUserTransactionsQueue, mostPurchasesUsersBatch)

	expectedMostPurchasesUsers := []*joined.JoinMostPurchasesUser{
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 260611},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 91218},
	}

	received := helpers.GetAllOutputMessages(t, middleware.GetJoinedUserTransactionsQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})[0]

	helpers.AssertJoinedMostPurchasesUsersIsExpected(t, received, expectedMostPurchasesUsers)
}

func TestHandleTaskType4Server(t *testing.T) {
	storeDir := t.TempDir()
	testCase := test_helpers.TestCase{
		QueueName:   "stores",
		Queue:       middleware.GetStoresQueue,
		DatasetType: enum.Stores,
		CsvPayloads: [][]byte{
			[]byte("5,G Coffee @ Seksyen 21,Jalan 1,12345,CityA,StateA,1.0,2.0\n"),
			[]byte("6,G Coffee @ Alam Tun Hussein Onn,Jalan 2,23456,CityB,StateB,3.0,4.0\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "stores.pb")},
		TaskDone:      enum.T4,
		SendDone:      true,
	}

	testCaseUsers := test_helpers.TestCase{
		QueueName:   "users",
		Queue:       middleware.GetUsersQueue,
		DatasetType: enum.Users,
		CsvPayloads: [][]byte{
			[]byte("1,female,1970-04-22,2023-07-01 08:13:07\n"),
			[]byte("2,female,1974-06-21,2023-08-01 08:13:07\n"),
		},
		ExpectedFiles: []string{filepath.Join(storeDir, "users_1-2.pb")},
		TaskDone:      enum.T4,
		SendDone:      true,
	}

	var wg sync.WaitGroup
	joinServer := helpers.InitServer(t, storeDir, &wg)
	defer func() {
		joinServer.Shutdown()
		wg.Wait()
	}()

	test_helpers.RunTest(t, testCase)
	test_helpers.RunTest(t, testCaseUsers)

	mostPurchasesUsers := []*reduced.MostPurchasesUser{
		{StoreId: 5, UserId: 1, PurchasesQty: 260611},
		{StoreId: 6, UserId: 2, PurchasesQty: 91218},
	}
	mostPurchasesUsersBatch := helpers.PrepareMostPurchasesUserBatch(t, mostPurchasesUsers, enum.T4)
	helpers.SendDataBatch(t, middleware.GetUserTransactionsQueue, mostPurchasesUsersBatch)

	expectedMostPurchasesUsers := []*joined.JoinMostPurchasesUser{
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 260611},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 91218},
	}

	received := helpers.GetAllOutputMessages(t, middleware.GetJoinedUserTransactionsQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})[0]

	helpers.AssertJoinedMostPurchasesUsersIsExpected(t, received, expectedMostPurchasesUsers)
}

func TestHandleConnectionGatewayController(t *testing.T) {
	storeDir := t.TempDir()

	var wg sync.WaitGroup
	joinServer := helpers.InitServer(t, storeDir, &wg)
	defer func() {
		joinServer.Shutdown()
		wg.Wait()
	}()

	helpers.AssertConnectionMsg(t, middleware.GetNodeConnectionsQueue, true)

	finishExchange := middleware.GetFinishExchange(helpers.RabbitURL, enum.Joiner)

	finishMsg := &data_batch.DataBatch{
		TaskType: int32(enum.T4),
		Done:     true,
	}

	dataMessage, err := proto.Marshal(finishMsg)
	assert.NoError(t, err)
	e := finishExchange.Send(dataMessage)
	assert.Equal(t, 0, int(e))

	helpers.AssertConnectionMsg(t, middleware.GetNodeConnectionsQueue, false)
}
