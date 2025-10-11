package cache_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var transactions = []*raw.Transaction{
	{
		TransactionId: "1",
		StoreId:       "store1",
		UserId:        "user1",
		FinalAmount:   100.0,
		CreatedAt:     "2025-10-01T10:00:00Z",
	},
	{
		TransactionId: "2",
		StoreId:       "store2",
		UserId:        "user2",
		FinalAmount:   200.0,
		CreatedAt:     "2025-10-03T12:00:00Z",
	},
	{
		TransactionId: "3",
		StoreId:       "store3",
		UserId:        "user3",
		FinalAmount:   150.0,
		CreatedAt:     "2025-10-02T11:00:00Z",
	},
}

var tpvData = []*reduced.TotalPaymentValue{
	{
		StoreId:     "store1",
		Semester:    "H1",
		FinalAmount: 3000.0,
	},
	{
		StoreId:     "store1",
		Semester:    "H1",
		FinalAmount: 3000.0,
	},
	{
		StoreId:     "store2",
		Semester:    "H1",
		FinalAmount: 2000.0,
	},
	{
		StoreId:     "store2",
		Semester:    "H1",
		FinalAmount: 2000.0,
	},
	{
		StoreId:     "store3",
		Semester:    "H2",
		FinalAmount: 1000.0,
	},
}

func TestStoreAndSortTransactions(t *testing.T) {
	cacheService := cache.NewInMemoryCache()

	cacheRef := "transactionCache"

	// Convert []*raw.Transaction to []*proto.Message
	protoTransactions := make([]*proto.Message, len(transactions))
	for i, tx := range transactions {
		msg := proto.Message(tx)
		protoTransactions[i] = &msg
	}

	// Define a sort function to sort by FinalAmount
	sortFn := func(a, b *proto.Message) bool {
		txA := (*a).(*raw.Transaction)
		txB := (*b).(*raw.Transaction)
		return txA.FinalAmount < txB.FinalAmount
	}

	// Store the transactions in the cache, sorted by FinalAmount
	for _, tx := range protoTransactions {
		err := cacheService.StoreAggregatedData(cacheRef, (*tx).(*raw.Transaction).TransactionId, tx, nil)
		assert.NoError(t, err)
	}

	err := cacheService.SortData(cacheRef, sortFn)
	assert.NoError(t, err)

	// Read back the sorted transactions
	readData, err := cacheService.ReadBatch(cacheRef, int32(len(transactions)))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(readData))

	// Verify the order is by FinalAmount
	assert.Equal(t, "1", (*readData[0]).(*raw.Transaction).TransactionId) // 100.0
	assert.Equal(t, "3", (*readData[1]).(*raw.Transaction).TransactionId) // 150.0
	assert.Equal(t, "2", (*readData[2]).(*raw.Transaction).TransactionId) // 200.0
}

func TestStoreAndReadUnsortedTransactions(t *testing.T) {
	cacheService := cache.NewInMemoryCache()

	cacheRef := "transactionCache"

	// Convert []*raw.Transaction to []*proto.Message
	protoTransactions := make([]*proto.Message, len(transactions))
	for i, tx := range transactions {
		msg := proto.Message(tx)
		protoTransactions[i] = &msg
	}

	// Store the transactions in the cache, sorted by FinalAmount
	err := cacheService.StoreBatch(cacheRef, protoTransactions)
	assert.NoError(t, err)

	// Read back the sorted transactions
	readData, err := cacheService.ReadBatch(cacheRef, int32(len(transactions)))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(readData))

	// Verify the order is by FinalAmount
	assert.Equal(t, "1", (*readData[0]).(*raw.Transaction).TransactionId)
	assert.Equal(t, "2", (*readData[1]).(*raw.Transaction).TransactionId)
	assert.Equal(t, "3", (*readData[2]).(*raw.Transaction).TransactionId)
}

func TestReadFromNonExistingCache(t *testing.T) {
	cacheService := cache.NewInMemoryCache()

	cacheRef := "nonExistentCache"

	// Attempt to read from a non-existent cache reference
	readData, err := cacheService.ReadBatch(cacheRef, 5)
	assert.Error(t, err)
	assert.Nil(t, readData)
}

func TestReadFromEmptyCache(t *testing.T) {
	cacheService := cache.NewInMemoryCache()

	cacheRef := "emptyCache"

	protoTransactions := make([]*proto.Message, len(transactions))
	for i, tx := range transactions {
		msg := proto.Message(tx)
		protoTransactions[i] = &msg
	}

	// i now have stored 3 transactions
	err := cacheService.StoreBatch(cacheRef, protoTransactions)

	// Attempt to read 5, will only get 3
	readData, err := cacheService.ReadBatch(cacheRef, 5)

	assert.NoError(t, err)
	assert.Equal(t, 3, len(readData))

	// Now read again, should be empty
	readMoreData, err := cacheService.ReadBatch(cacheRef, 5)

	assert.Equal(t, 0, len(readMoreData))
	assert.NoError(t, err)
	assert.Nil(t, readMoreData)
}

func TestAggregateTPV(t *testing.T) {
	cacheService := cache.NewInMemoryCache()

	cacheRef := "transactionCache"

	// Convert []*raw.Transaction to []*proto.Message
	protoTPV := make([]*proto.Message, len(tpvData))
	for i, tx := range tpvData {
		msg := proto.Message(tx)
		protoTPV[i] = &msg
	}

	joinFn := func(existing, new *proto.Message) (*proto.Message, error) {
		existingTPV := (*existing).(*reduced.TotalPaymentValue)
		newTPV := (*new).(*reduced.TotalPaymentValue)
		// Aggregate the FinalAmount
		existingTPV.FinalAmount += newTPV.FinalAmount
		return existing, nil
	}

	// Define a sort function to sort by FinalAmount
	sortFn := func(a, b *proto.Message) bool {
		txA := (*a).(*reduced.TotalPaymentValue)
		txB := (*b).(*reduced.TotalPaymentValue)
		return txA.FinalAmount > txB.FinalAmount
	}

	// Store the transactions in the cache, sorted by FinalAmount
	for _, tx := range protoTPV {
		keyID := (*tx).(*reduced.TotalPaymentValue).StoreId + "@" + (*tx).(*reduced.TotalPaymentValue).Semester
		err := cacheService.StoreAggregatedData(cacheRef, keyID, tx, joinFn)
		assert.NoError(t, err)
	}

	err := cacheService.SortData(cacheRef, sortFn)
	assert.NoError(t, err)

	// Read back the sorted transactions
	readData, err := cacheService.ReadBatch(cacheRef, int32(len(transactions)))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(readData))

	// Convert readData back into TPV data
	tpvResults := make([]*reduced.TotalPaymentValue, len(readData))
	for i, msg := range readData {
		tx, ok := (*msg).(*reduced.TotalPaymentValue)
		assert.True(t, ok)
		tpvResults[i] = tx
	}

	assert.Equal(t, 6000.0, tpvResults[0].FinalAmount)
	assert.Equal(t, "store1", tpvResults[0].StoreId)
	assert.Equal(t, "H1", tpvResults[0].Semester)

	assert.Equal(t, 4000.0, tpvResults[1].FinalAmount)
	assert.Equal(t, "store2", tpvResults[1].StoreId)
	assert.Equal(t, "H1", tpvResults[1].Semester)

	assert.Equal(t, 1000.0, tpvResults[2].FinalAmount)
	assert.Equal(t, "store3", tpvResults[2].StoreId)
	assert.Equal(t, "H2", tpvResults[2].Semester)

}
