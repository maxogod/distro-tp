package business_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	cache "github.com/maxogod/distro-tp/src/common/worker/cache/disk_memory"
	"github.com/stretchr/testify/assert"
)

// ====== INPUT DATA ======

var Transactions = []*raw.Transaction{
	{
		TransactionId: "1",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 06:01:00",
	},
	{
		TransactionId: "1",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 06:00:00",
	},
	{
		TransactionId: "2",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 23:00:00",
	},
}

var UnsortedTransactionsMap = map[string]*raw.Transaction{
	"1": {
		TransactionId: "1",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 06:01:00",
	},
	"4": {
		TransactionId: "4",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 23:00:00",
	},
	"3": {
		TransactionId: "3",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 23:00:00",
	},
	"2": {
		TransactionId: "2",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 06:00:00",
	},
}

var TPVData = []*reduced.TotalPaymentValue{
	{
		StoreId:     "store1",
		Semester:    "2024-H1",
		FinalAmount: 1000.0,
	},
	{
		StoreId:     "store2",
		Semester:    "2024-H1",
		FinalAmount: 2000.0,
	},
	{
		StoreId:     "store1",
		Semester:    "2024-H1",
		FinalAmount: 1000.0,
	},
	{
		StoreId:     "store2",
		Semester:    "2024-H1",
		FinalAmount: 2000.0,
	},
}

var TotalItemSumData = []*reduced.TotalSumItem{
	{
		ItemId:    "item1",
		YearMonth: "2024-06",
		Subtotal:  10.0,
		Quantity:  10,
	},
	{
		ItemId:    "item2",
		YearMonth: "2024-07",
		Subtotal:  20.0,
		Quantity:  20,
	},
	{
		ItemId:    "item1",
		YearMonth: "2024-06",
		Subtotal:  15.0,
		Quantity:  15,
	},
}

var CountedUserTransactionsData = []*reduced.CountedUserTransactions{
	{
		StoreId:             "store1",
		UserId:              "user1",
		TransactionQuantity: 5,
	},
	{
		StoreId:             "store2",
		UserId:              "user2",
		TransactionQuantity: 10,
	},
	{
		StoreId:             "store1",
		UserId:              "user1",
		TransactionQuantity: 3,
	},
}

// ====== EXPECTED DATA ======

var TPVExpected = map[string]float64{
	"store1@2024-H1": 2000.0,
	"store2@2024-H1": 4000.0,
}

var TQuantityExpected = map[string]int32{
	"item1@2024-06": 25,
	"item2@2024-07": 20,
}

var TSubtotalExpected = map[string]float64{
	"item1@2024-06": 25.0,
	"item2@2024-07": 20.0,
}

var CUTExpected = map[string]int32{
	"store1@user1": 8,
	"store2@user2": 10,
}

// ====== TESTS ======

func TestAggregatorService_StoreAndReadTransactions(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	clientID := "client1"
	c := cache.NewDiskMemoryStorage()
	service := business.NewAggregatorService(c)
	defer service.RemoveData(clientID)

	err := service.StoreTransactions(clientID, Transactions)
	assert.NoError(t, err)

	results, err := service.GetStoredTransactions(clientID)
	assert.NoError(t, err)

	assert.Len(t, results, len(Transactions))

	for i, expected := range Transactions {
		assert.True(t, expected.TransactionId == results[i].TransactionId &&
			expected.FinalAmount == results[i].FinalAmount,
			"Transaction mismatch at index %d", i)
	}
}

func TestAggregatorService_StoreAggregatedAndReadTotalSoldQuantity(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)

	clientID := "client2"
	c := cache.NewDiskMemoryStorage()
	service := business.NewAggregatorService(c)
	defer service.RemoveData(clientID)

	for _, sq := range TotalItemSumData {
		err := service.StoreTotalItems(clientID, sq)
		assert.NoError(t, err)
	}

	subtotalResults, quantityResults, err := service.GetStoredTotalItems(clientID)
	assert.NoError(t, err)

	assert.Len(t, subtotalResults, len(TSubtotalExpected))
	assert.Len(t, quantityResults, len(TQuantityExpected))

	for _, result := range subtotalResults {
		key := result.ItemId + "@" + result.YearMonth
		expectedAmount, ok := TSubtotalExpected[key]
		assert.True(t, ok, "Unexpected key found: %s", key)
		assert.Equal(t, expectedAmount, result.Subtotal,
			"Subtotal mismatch for key %s", key)
	}
	for _, result := range quantityResults {
		key := result.ItemId + "@" + result.YearMonth
		expectedAmount, ok := TQuantityExpected[key]
		assert.True(t, ok, "Unexpected key found: %s", key)
		assert.Equal(t, expectedAmount, result.Quantity,
			"Quantity mismatch for key %s", key)
	}
}

func TestAggregatorService_StoreAggregatedAndReadTpvData(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)

	clientID := "client3"
	c := cache.NewDiskMemoryStorage()
	service := business.NewAggregatorService(c)
	defer service.RemoveData(clientID)

	for _, tpv := range TPVData {
		err := service.StoreTotalPaymentValue(clientID, tpv)
		assert.NoError(t, err)
	}

	results, err := service.GetStoredTotalPaymentValue(clientID)
	assert.NoError(t, err)

	assert.Len(t, results, len(TPVExpected))

	for _, result := range results {
		key := result.StoreId + "@" + result.Semester
		expectedAmount, ok := TPVExpected[key]
		assert.True(t, ok, "Unexpected key found: %s", key)
		assert.Equal(t, expectedAmount, result.FinalAmount,
			"FinalAmount mismatch for key %s", key)
	}
}

func TestAggregatorService_StoreAggregatedAndReadTotalCountedUserTransactions(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)

	clientID := "client4"
	c := cache.NewDiskMemoryStorage()
	service := business.NewAggregatorService(c)
	defer service.RemoveData(clientID)

	for _, cut := range CountedUserTransactionsData {
		err := service.StoreCountedUserTransactions(clientID, cut)
		assert.NoError(t, err)
	}

	results, err := service.GetStoredCountedUserTransactions(clientID)
	assert.NoError(t, err)

	assert.Len(t, results, len(CUTExpected))

	for _, subResult := range results {
		assert.Len(t, subResult, 1, "Expected only one entry per store")
		result := subResult[0]
		key := result.StoreId + "@" + result.UserId
		expectedAmount, ok := CUTExpected[key]
		assert.True(t, ok, "Unexpected key found: %s", key)
		assert.Equal(t, expectedAmount, result.TransactionQuantity,
			"TransactionQuantity mismatch for key %s", key)
	}
}
