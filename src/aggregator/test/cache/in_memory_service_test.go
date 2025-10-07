package cache_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/models/raw"
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
		txA := (*a).(*raw.Transaction) // Cast back to *raw.Transaction
		txB := (*b).(*raw.Transaction)
		return txA.FinalAmount < txB.FinalAmount
	}

	// Store the transactions in the cache, sorted by FinalAmount
	err := cacheService.StoreSortedBatch(cacheRef, protoTransactions, sortFn)
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

	assert.Error(t, err)
	assert.Nil(t, readMoreData)
}

func TestRemoveCache(t *testing.T) {
	cacheService := cache.NewInMemoryCache()

	cacheRef := "transactionCacheToRemove"

	protoTransactions := make([]*proto.Message, len(transactions))
	for i, tx := range transactions {
		msg := proto.Message(tx)
		protoTransactions[i] = &msg
	}

	// i now have stored 3 transactions
	err := cacheService.StoreBatch(cacheRef, protoTransactions)
	assert.NoError(t, err)

	// Remove the cache
	err = cacheService.Remove(cacheRef)
	assert.NoError(t, err)

	// Attempt to read from the removed cache
	readData, err := cacheService.ReadBatch(cacheRef, 5)
	assert.Error(t, err)
	assert.Nil(t, readData)
}
