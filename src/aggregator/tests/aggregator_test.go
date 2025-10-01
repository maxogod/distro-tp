package tests

import (
	"sync"
	"testing"

	aggregator "github.com/maxogod/distro-tp/src/aggregator/business"
	helpers "github.com/maxogod/distro-tp/src/aggregator/tests/test_helpers"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestHandleTaskType4(t *testing.T) {
	storeDir := t.TempDir()

	mostPurchasesUsers := []*joined.JoinMostPurchasesUser{
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 260611},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 91218},
	}
	mostPurchasesUsersBatch := helpers.PrepareJoinMostPurchasesUserBatch(t, mostPurchasesUsers, enum.T4)

	testCase := helpers.CreateTestCaseTask4(storeDir, mostPurchasesUsersBatch, true)

	agg := helpers.StartAggregator(t, storeDir, []string{testCase.Queue})
	defer func(agg *aggregator.Aggregator) {
		err := agg.Stop()
		assert.NoError(t, err)
	}(agg)

	helpers.RunTest(t, testCase)

	received := helpers.GetAllOutputMessages(t, testCase.AggregatorConfig.GatewayControllerDataQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})

	helpers.AssertAggregatedMostPurchasesUsers(t, received[0], mostPurchasesUsers)

	doneDataMsg := received[1]
	assert.Equal(t, int32(enum.T4), doneDataMsg.TaskType)
	assert.Equal(t, true, doneDataMsg.Done)
}

func TestHandleConnection(t *testing.T) {
	storeDir := t.TempDir()

	aggregatorConfig := helpers.AggregatorConfig(storeDir)

	agg := aggregator.NewAggregator(&aggregatorConfig)

	err := agg.InitService()
	assert.NoError(t, err)

	helpers.AssertConnectionMsg(t, aggregatorConfig.GatewayControllerConnectionQueue, false)

	err = agg.Stop()
	assert.NoError(t, err)
}

func TestHandleTaskType4Top3(t *testing.T) {
	storeDir := t.TempDir()

	mostPurchasesUsers := []*joined.JoinMostPurchasesUser{
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 1},

		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-06-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-06-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-06-21", PurchasesQty: 1},

		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-05-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-05-21", PurchasesQty: 1},

		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1971-05-21", PurchasesQty: 1},

		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1970-04-22", PurchasesQty: 1},

		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 1},

		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-05-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-05-21", PurchasesQty: 1},

		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1971-05-21", PurchasesQty: 1},
	}
	mostPurchasesUsersBatch := helpers.PrepareJoinMostPurchasesUserBatch(t, mostPurchasesUsers, enum.T4)

	testCase := helpers.CreateTestCaseTask4(storeDir, mostPurchasesUsersBatch, true)

	agg := helpers.StartAggregator(t, storeDir, []string{testCase.Queue})
	defer func(agg *aggregator.Aggregator) {
		err := agg.Stop()
		assert.NoError(t, err)
	}(agg)

	helpers.RunTest(t, testCase)

	received := helpers.GetAllOutputMessages(t, testCase.AggregatorConfig.GatewayControllerDataQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})

	expectedMostPurchases := []*joined.JoinMostPurchasesUser{
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 4},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-06-21", PurchasesQty: 3},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-05-21", PurchasesQty: 2},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1970-04-22", PurchasesQty: 4},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 3},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-05-21", PurchasesQty: 2},
	}

	helpers.AssertAggregatedMostPurchasesUsers(t, received[0], expectedMostPurchases)

	doneDataMsg := received[1]
	assert.Equal(t, int32(enum.T4), doneDataMsg.TaskType)
	assert.Equal(t, true, doneDataMsg.Done)
}

func TestHandleTaskType3(t *testing.T) {
	storeDir := t.TempDir()

	storeTPVs := []*joined.JoinStoreTPV{
		{YearHalfCreatedAt: "2024-H1", StoreName: "G Coffee @ Seksyen 21", Tpv: 1},
		{YearHalfCreatedAt: "2024-H1", StoreName: "G Coffee @ Seksyen 21", Tpv: 1},
		{YearHalfCreatedAt: "2024-H1", StoreName: "G Coffee @ Seksyen 21", Tpv: 1},
		{YearHalfCreatedAt: "2024-H1", StoreName: "G Coffee @ Alam Tun Hussein Onn", Tpv: 1},
		{YearHalfCreatedAt: "2024-H1", StoreName: "G Coffee @ Alam Tun Hussein Onn", Tpv: 1},
		{YearHalfCreatedAt: "2025-H1", StoreName: "G Coffee @ Kampung Changkat", Tpv: 1},
	}
	storeTPVBatch := helpers.PrepareJoinStoreTPVBatch(t, storeTPVs, enum.T3)

	testCase := helpers.CreateTestCaseTask3(storeDir, storeTPVBatch, true)

	agg := helpers.StartAggregator(t, storeDir, []string{testCase.Queue})
	defer func(agg *aggregator.Aggregator) {
		err := agg.Stop()
		assert.NoError(t, err)
	}(agg)

	helpers.RunTest(t, testCase)

	received := helpers.GetAllOutputMessages(t, testCase.AggregatorConfig.GatewayControllerDataQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})

	expectedStoresTPVs := []*joined.JoinStoreTPV{
		{YearHalfCreatedAt: "2024-H1", StoreName: "G Coffee @ Seksyen 21", Tpv: 3},
		{YearHalfCreatedAt: "2024-H1", StoreName: "G Coffee @ Alam Tun Hussein Onn", Tpv: 2},
		{YearHalfCreatedAt: "2025-H1", StoreName: "G Coffee @ Kampung Changkat", Tpv: 1},
	}

	helpers.AssertAggregatedStoresTPV(t, received[0], expectedStoresTPVs)

	doneDataMsg := received[1]
	assert.Equal(t, int32(enum.T3), doneDataMsg.TaskType)
	assert.Equal(t, true, doneDataMsg.Done)
}

func TestHandleTaskType2(t *testing.T) {
	storeDir := t.TempDir()

	bestSelling := []*joined.JoinBestSellingProducts{
		{YearMonthCreatedAt: "2024-01", ItemName: "Espresso", SellingsQty: 1},
		{YearMonthCreatedAt: "2024-01", ItemName: "Espresso", SellingsQty: 1},
		{YearMonthCreatedAt: "2024-01", ItemName: "Espresso", SellingsQty: 1},
		{YearMonthCreatedAt: "2024-01", ItemName: "Americano", SellingsQty: 1},
		{YearMonthCreatedAt: "2024-01", ItemName: "Americano", SellingsQty: 1},

		{YearMonthCreatedAt: "2024-02", ItemName: "Espresso", SellingsQty: 1},
		{YearMonthCreatedAt: "2024-02", ItemName: "Espresso", SellingsQty: 1},
		{YearMonthCreatedAt: "2024-02", ItemName: "Americano", SellingsQty: 1},
		{YearMonthCreatedAt: "2024-02", ItemName: "Americano", SellingsQty: 1},
		{YearMonthCreatedAt: "2024-02", ItemName: "Americano", SellingsQty: 1},
	}

	mostProfits := []*joined.JoinMostProfitsProducts{
		{YearMonthCreatedAt: "2024-01", ItemName: "Espresso", ProfitSum: 1},
		{YearMonthCreatedAt: "2024-01", ItemName: "Espresso", ProfitSum: 1},
		{YearMonthCreatedAt: "2024-01", ItemName: "Espresso", ProfitSum: 1},
		{YearMonthCreatedAt: "2024-01", ItemName: "Americano", ProfitSum: 1},
		{YearMonthCreatedAt: "2024-01", ItemName: "Americano", ProfitSum: 1},

		{YearMonthCreatedAt: "2024-02", ItemName: "Espresso", ProfitSum: 1},
		{YearMonthCreatedAt: "2024-02", ItemName: "Espresso", ProfitSum: 1},
		{YearMonthCreatedAt: "2024-02", ItemName: "Americano", ProfitSum: 1},
		{YearMonthCreatedAt: "2024-02", ItemName: "Americano", ProfitSum: 1},
		{YearMonthCreatedAt: "2024-02", ItemName: "Americano", ProfitSum: 1},
	}

	bestSellingBatch := helpers.PrepareJoinBestSellingBatch(t, bestSelling, enum.T2)
	mostProfitsBatch := helpers.PrepareJoinMostProfitsBatch(t, mostProfits, enum.T2)

	testCaseBestSelling := helpers.CreateTestCaseTask2(storeDir, bestSellingBatch, "task2_1", true, false)
	testCaseMostProfits := helpers.CreateTestCaseTask2(storeDir, mostProfitsBatch, "task2_2", false, true)

	agg := helpers.StartAggregator(t, storeDir, []string{testCaseBestSelling.Queue, testCaseMostProfits.Queue})
	defer func(agg *aggregator.Aggregator) {
		err := agg.Stop()
		assert.NoError(t, err)
	}(agg)

	helpers.RunTest(t, testCaseBestSelling)
	helpers.RunTest(t, testCaseMostProfits)

	received := helpers.GetAllOutputMessages(t, testCaseBestSelling.AggregatorConfig.GatewayControllerDataQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})

	expectedBestSelling := []*joined.JoinBestSellingProducts{
		{YearMonthCreatedAt: "2024-01", ItemName: "Espresso", SellingsQty: 3},
		{YearMonthCreatedAt: "2024-02", ItemName: "Americano", SellingsQty: 3},
	}

	expectedMostProfits := []*joined.JoinMostProfitsProducts{
		{YearMonthCreatedAt: "2024-01", ItemName: "Espresso", ProfitSum: 3},
		{YearMonthCreatedAt: "2024-02", ItemName: "Americano", ProfitSum: 3},
	}

	helpers.AssertAggregatedBestSelling(t, received[0], expectedBestSelling)

	doneDataMsg := received[1]
	assert.Equal(t, int32(enum.T2), doneDataMsg.TaskType)
	assert.Equal(t, true, doneDataMsg.Done)

	helpers.AssertAggregatedMostProfits(t, received[2], expectedMostProfits)

	doneDataMsg = received[3]
	assert.Equal(t, int32(enum.T2), doneDataMsg.TaskType)
	assert.Equal(t, true, doneDataMsg.Done)
}

func TestHandleTaskServer(t *testing.T) {
	storeDir := t.TempDir()

	mostPurchasesUsers := []*joined.JoinMostPurchasesUser{
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 1},

		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-06-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-06-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-06-21", PurchasesQty: 1},

		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-05-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-05-21", PurchasesQty: 1},

		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1971-05-21", PurchasesQty: 1},

		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1970-04-22", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1970-04-22", PurchasesQty: 1},

		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 1},

		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-05-21", PurchasesQty: 1},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-05-21", PurchasesQty: 1},

		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1971-05-21", PurchasesQty: 1},
	}
	mostPurchasesUsersBatch := helpers.PrepareJoinMostPurchasesUserBatch(t, mostPurchasesUsers, enum.T4)

	testCase := helpers.CreateTestCaseTask4(storeDir, mostPurchasesUsersBatch, true)

	var wg sync.WaitGroup
	aggServer := helpers.InitServer(t, storeDir, &wg)
	defer func() {
		aggServer.Shutdown()
		wg.Wait()
	}()

	helpers.RunTest(t, testCase)

	received := helpers.GetAllOutputMessages(t, testCase.AggregatorConfig.GatewayControllerDataQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})

	expectedMostPurchases := []*joined.JoinMostPurchasesUser{
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1970-04-22", PurchasesQty: 4},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-06-21", PurchasesQty: 3},
		{StoreName: "G Coffee @ Seksyen 21", UserBirthdate: "1974-05-21", PurchasesQty: 2},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1970-04-22", PurchasesQty: 4},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-06-21", PurchasesQty: 3},
		{StoreName: "G Coffee @ Alam Tun Hussein Onn", UserBirthdate: "1974-05-21", PurchasesQty: 2},
	}

	helpers.AssertAggregatedMostPurchasesUsers(t, received[0], expectedMostPurchases)

	doneDataMsg := received[1]
	assert.Equal(t, int32(enum.T4), doneDataMsg.TaskType)
	assert.Equal(t, true, doneDataMsg.Done)
}

func TestHandleTaskType1(t *testing.T) {
	storeDir := t.TempDir()

	transactions := []*raw.Transaction{
		{
			TransactionId:   "tx-001",
			StoreId:         101,
			PaymentMethod:   1,
			VoucherId:       0,
			UserId:          501,
			OriginalAmount:  10.00,
			DiscountApplied: 0.00,
			FinalAmount:     10.00,
			CreatedAt:       "2024-01-15T10:05:00Z",
		},
		{
			TransactionId:   "tx-002",
			StoreId:         101,
			PaymentMethod:   1,
			VoucherId:       1234,
			UserId:          502,
			OriginalAmount:  15.00,
			DiscountApplied: 5.00,
			FinalAmount:     10.00,
			CreatedAt:       "2024-02-10T14:20:00Z",
		},
	}

	transactionsBatch := helpers.PrepareTransactionsBatch(t, transactions, enum.T1)

	testCase := helpers.CreateTestCaseTask1(storeDir, transactionsBatch, true)

	agg := helpers.StartAggregator(t, storeDir, []string{testCase.Queue})
	defer func(agg *aggregator.Aggregator) {
		err := agg.Stop()
		assert.NoError(t, err)
	}(agg)

	helpers.RunTest(t, testCase)

	received := helpers.GetAllOutputMessages(t, testCase.AggregatorConfig.GatewayControllerDataQueue, func(body []byte) (*data_batch.DataBatch, error) {
		batch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(body, batch); err != nil {
			return nil, err
		}
		return batch, nil
	})

	helpers.AssertAggregatedTransactions(t, received[0], transactions)

	doneDataMsg := received[1]
	assert.Equal(t, int32(enum.T3), doneDataMsg.TaskType)
	assert.Equal(t, true, doneDataMsg.Done)
}
