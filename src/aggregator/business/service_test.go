package business_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
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

var TotalSoldByQuantityData = []*reduced.TotalSoldByQuantity{
	{
		ItemId:    "item1",
		YearMonth: "2024-06",
		Quantity:  10,
	},
	{
		ItemId:    "item2",
		YearMonth: "2024-06",
		Quantity:  20,
	},
	{
		ItemId:    "item1",
		YearMonth: "2024-06",
		Quantity:  15,
	},
}

var TotalProfitBySubtotalData = []*reduced.TotalProfitBySubtotal{
	{
		ItemId:    "item1",
		YearMonth: "2024-06",
		Subtotal:  10.0,
	},
	{
		ItemId:    "item2",
		YearMonth: "2024-06",
		Subtotal:  20.0,
	},
	{
		ItemId:    "item1",
		YearMonth: "2024-06",
		Subtotal:  15.0,
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

var TSQExpected = map[string]int32{
	"item1@2024-06": 25,
	"item2@2024-06": 20,
}

var TPBSEpected = map[string]float64{
	"item1@2024-06": 25.0,
	"item2@2024-06": 20.0,
}

var CUTExpected = map[string]int32{
	"store1@user1": 8,
	"store2@user2": 10,
}

// ====== TESTS ======

func TestAggregatorService_StoreAndReadTransactions(t *testing.T) {

	clientID := "client1"
	c := cache.NewDiskMemoryCache()
	service := business.NewAggregatorService(c)
	defer service.FinishData(clientID)

	err := service.StoreTransactions(clientID, Transactions)
	assert.NoError(t, err)

	results, found := service.GetStoredTransactions(clientID, 10)
	assert.True(t, found)

	assert.Len(t, results, len(Transactions))

	for i, expected := range Transactions {
		assert.True(t, expected.TransactionId == results[i].TransactionId &&
			expected.FinalAmount == results[i].FinalAmount,
			"Transaction mismatch at index %d", i)
	}
}

func TestAggregatorService_StoreAggregatedAndReadTpvData(t *testing.T) {

	clientID := "client2"
	c := cache.NewDiskMemoryCache()
	service := business.NewAggregatorService(c)
	defer service.FinishData(clientID)

	for _, tpv := range TPVData {
		err := service.StoreTotalPaymentValue(clientID, tpv)
		assert.NoError(t, err)
	}

	results, found := service.GetStoredTotalPaymentValue(clientID, 10)
	assert.True(t, found)

	assert.Len(t, results, len(TPVExpected))

	for _, result := range results {
		key := result.StoreId + "@" + result.Semester
		expectedAmount, ok := TPVExpected[key]
		assert.True(t, ok, "Unexpected key found: %s", key)
		assert.Equal(t, expectedAmount, result.FinalAmount,
			"FinalAmount mismatch for key %s", key)
	}
}

func TestAggregatorService_StoreAggregatedAndReadTotalSoldQuantity(t *testing.T) {

	clientID := "client3"
	c := cache.NewDiskMemoryCache()
	service := business.NewAggregatorService(c)
	defer service.FinishData(clientID)

	for _, sq := range TotalSoldByQuantityData {
		err := service.StoreTotalSoldByQuantity(clientID, sq)
		assert.NoError(t, err)
	}

	results, found := service.GetStoredTotalSoldByQuantity(clientID, 10)
	assert.True(t, found)

	assert.Len(t, results, len(TPVExpected))

	for _, result := range results {
		key := result.ItemId + "@" + result.YearMonth
		expectedAmount, ok := TSQExpected[key]
		assert.True(t, ok, "Unexpected key found: %s", key)
		assert.Equal(t, expectedAmount, result.Quantity,
			"Quantity mismatch for key %s", key)
	}
}

func TestAggregatorService_StoreAggregatedAndReadTotalProfitBySubtotal(t *testing.T) {

	clientID := "client4"
	c := cache.NewDiskMemoryCache()
	service := business.NewAggregatorService(c)
	defer service.FinishData(clientID)

	for _, ps := range TotalProfitBySubtotalData {
		err := service.StoreTotalProfitBySubtotal(clientID, ps)
		assert.NoError(t, err)
	}

	results, found := service.GetStoredTotalProfitBySubtotal(clientID, 10)
	assert.True(t, found)

	assert.Len(t, results, len(TPBSEpected))

	for _, result := range results {
		key := result.ItemId + "@" + result.YearMonth
		expectedAmount, ok := TPBSEpected[key]
		assert.True(t, ok, "Unexpected key found: %s", key)
		assert.Equal(t, expectedAmount, result.Subtotal,
			"Subtotal mismatch for key %s", key)
	}
}

func TestAggregatorService_StoreAggregatedAndReadTotalCountedUserTransactions(t *testing.T) {

	clientID := "client5"
	c := cache.NewDiskMemoryCache()
	service := business.NewAggregatorService(c)
	defer service.FinishData(clientID)

	for _, cut := range CountedUserTransactionsData {
		err := service.StoreCountedUserTransactions(clientID, cut)
		assert.NoError(t, err)
	}

	results, found := service.GetStoredCountedUserTransactions(clientID, 10)
	assert.True(t, found)

	assert.Len(t, results, len(CUTExpected))

	for _, result := range results {
		key := result.StoreId + "@" + result.UserId
		expectedAmount, ok := CUTExpected[key]
		assert.True(t, ok, "Unexpected key found: %s", key)
		assert.Equal(t, expectedAmount, result.TransactionQuantity,
			"TransactionQuantity mismatch for key %s", key)
	}
}

func TestAggregatorService_SortAggregatedData(t *testing.T) {

	clientID := "client6"
	c := cache.NewDiskMemoryCache()
	service := business.NewAggregatorService(c)
	defer service.FinishData(clientID)

	var unsortedTransactions []*raw.Transaction
	for _, tx := range UnsortedTransactionsMap {
		unsortedTransactions = append(unsortedTransactions, tx)
	}

	err := service.StoreTransactions(clientID, unsortedTransactions)
	assert.NoError(t, err)

	service.SortData(clientID, func(a, b []byte) bool {
		txA := &raw.Transaction{}
		txB := &raw.Transaction{}
		err := proto.Unmarshal(a, txA)
		assert.NoError(t, err)
		err = proto.Unmarshal(b, txB)
		assert.NoError(t, err)

		return txA.TransactionId < txB.TransactionId
	})

	results, found := service.GetStoredTransactions(clientID, 10)
	assert.True(t, found)
	for _, result := range results {
		expected, _ := UnsortedTransactionsMap[result.TransactionId]
		assert.Equal(t, expected.TransactionId, result.TransactionId,
			"TransactionId mismatch for TransactionId %s", result.TransactionId)
	}
}
