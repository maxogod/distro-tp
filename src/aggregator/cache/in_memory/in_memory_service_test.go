package in_memory_test

// import (
// 	"testing"

// 	"github.com/maxogod/distro-tp/src/aggregator/cache"
// 	"github.com/maxogod/distro-tp/src/common/models/raw"
// 	"github.com/maxogod/distro-tp/src/common/models/reduced"
// 	"github.com/maxogod/distro-tp/src/common/utils"
// 	"github.com/stretchr/testify/assert"
// 	"google.golang.org/protobuf/proto"
// )

// var transactions = []*raw.Transaction{
// 	{
// 		TransactionId: "1",
// 		StoreId:       "store1",
// 		UserId:        "user1",
// 		FinalAmount:   100.0,
// 		CreatedAt:     "2025-10-01T10:00:00Z",
// 	},
// 	{
// 		TransactionId: "2",
// 		StoreId:       "store2",
// 		UserId:        "user2",
// 		FinalAmount:   200.0,
// 		CreatedAt:     "2025-10-03T12:00:00Z",
// 	},
// 	{
// 		TransactionId: "3",
// 		StoreId:       "store3",
// 		UserId:        "user3",
// 		FinalAmount:   150.0,
// 		CreatedAt:     "2025-10-02T11:00:00Z",
// 	},
// }

// var tpvData = []*reduced.TotalPaymentValue{
// 	{
// 		StoreId:     "store1",
// 		Semester:    "H1",
// 		FinalAmount: 3000.0,
// 	},
// 	{
// 		StoreId:     "store1",
// 		Semester:    "H1",
// 		FinalAmount: 3000.0,
// 	},
// 	{
// 		StoreId:     "store2",
// 		Semester:    "H1",
// 		FinalAmount: 2000.0,
// 	},
// 	{
// 		StoreId:     "store2",
// 		Semester:    "H1",
// 		FinalAmount: 2000.0,
// 	},
// 	{
// 		StoreId:     "store3",
// 		Semester:    "H2",
// 		FinalAmount: 1000.0,
// 	},
// }

// func TestStoreAndSortTransactions(t *testing.T) {
// 	cacheService := cache.NewInMemoryCache()

// 	cacheRef := "transactionCache"

// 	// Convert []*raw.Transaction to []proto.Message
// 	protoTransactions := make([]proto.Message, len(transactions))
// 	for i, tx := range transactions {
// 		protoTransactions[i] = tx
// 	}

// 	// Define a sort function to sort by FinalAmount
// 	sortFn := func(a, b proto.Message) bool {
// 		txA := utils.CastProtoMessage[*raw.Transaction](a)
// 		txB := utils.CastProtoMessage[*raw.Transaction](b)
// 		return txA.FinalAmount < txB.FinalAmount
// 	}

// 	// Store the transactions in the cache, sorted by FinalAmount
// 	for _, tx := range protoTransactions {
// 		err := cacheService.StoreAggregatedData(cacheRef, utils.CastProtoMessage[*raw.Transaction](tx).TransactionId, tx, nil)
// 		assert.NoError(t, err)
// 	}

// 	err := cacheService.SortData(cacheRef, sortFn)
// 	assert.NoError(t, err)

// 	// Read back the sorted transactions
// 	readData, err := cacheService.ReadBatch(cacheRef, int32(len(transactions)))
// 	assert.NoError(t, err)
// 	assert.Equal(t, 3, len(readData))

// 	// Verify the order is by FinalAmount
// 	assert.Equal(t, "1", utils.CastProtoMessage[*raw.Transaction](readData[0]).TransactionId) // 100.0
// 	assert.Equal(t, "3", utils.CastProtoMessage[*raw.Transaction](readData[1]).TransactionId) // 150.0
// 	assert.Equal(t, "2", utils.CastProtoMessage[*raw.Transaction](readData[2]).TransactionId) // 200.0
// }

// func TestStoreAndReadUnsortedTransactions(t *testing.T) {
// 	cacheService := cache.NewInMemoryCache()

// 	cacheRef := "transactionCache"

// 	// Convert []*raw.Transaction to []proto.Message
// 	protoTransactions := make([]proto.Message, len(transactions))
// 	for i, tx := range transactions {
// 		protoTransactions[i] = tx
// 	}

// 	// Store the transactions in the cache, sorted by FinalAmount
// 	err := cacheService.StoreBatch(cacheRef, protoTransactions)
// 	assert.NoError(t, err)

// 	// Read back the sorted transactions
// 	readData, err := cacheService.ReadBatch(cacheRef, int32(len(transactions)))
// 	assert.NoError(t, err)
// 	assert.Equal(t, 3, len(readData))

// 	// Verify the order is by FinalAmount
// 	assert.Equal(t, "1", utils.CastProtoMessage[*raw.Transaction](readData[0]).TransactionId)
// 	assert.Equal(t, "2", utils.CastProtoMessage[*raw.Transaction](readData[1]).TransactionId)
// 	assert.Equal(t, "3", utils.CastProtoMessage[*raw.Transaction](readData[2]).TransactionId)
// }

// func TestReadFromNonExistingCache(t *testing.T) {
// 	cacheService := cache.NewInMemoryCache()

// 	cacheRef := "nonExistentCache"

// 	// Attempt to read from a non-existent cache reference
// 	readData, err := cacheService.ReadBatch(cacheRef, 5)
// 	assert.Error(t, err)
// 	assert.Nil(t, readData)
// }

// func TestReadFromEmptyCache(t *testing.T) {
// 	cacheService := cache.NewInMemoryCache()

// 	cacheRef := "emptyCache"

// 	protoTransactions := make([]proto.Message, len(transactions))
// 	for i, tx := range transactions {
// 		protoTransactions[i] = tx
// 	}

// 	// i now have stored 3 transactions
// 	err := cacheService.StoreBatch(cacheRef, protoTransactions)

// 	// Attempt to read 5, will only get 3
// 	readData, err := cacheService.ReadBatch(cacheRef, 5)

// 	assert.NoError(t, err)
// 	assert.Equal(t, 3, len(readData))

// 	// Now read again, should be empty
// 	readMoreData, err := cacheService.ReadBatch(cacheRef, 5)

// 	assert.Equal(t, 0, len(readMoreData))
// 	assert.NoError(t, err)
// 	assert.Nil(t, readMoreData)
// }

// func TestAggregateTPV(t *testing.T) {
// 	cacheService := cache.NewInMemoryCache()

// 	cacheRef := "transactionCache"

// 	// Convert []*raw.Transaction to []proto.Message
// 	protoTPV := make([]proto.Message, len(tpvData))
// 	for i, tx := range tpvData {
// 		protoTPV[i] = tx
// 	}

// 	joinFn := func(existing, new proto.Message) (proto.Message, error) {
// 		existingTPV := utils.CastProtoMessage[*reduced.TotalPaymentValue](existing)
// 		newTPV := utils.CastProtoMessage[*reduced.TotalPaymentValue](new)
// 		// Aggregate the FinalAmount
// 		existingTPV.FinalAmount += newTPV.FinalAmount
// 		return existing, nil
// 	}

// 	// Define a sort function to sort by FinalAmount
// 	sortFn := func(a, b proto.Message) bool {
// 		txA := utils.CastProtoMessage[*reduced.TotalPaymentValue](a)
// 		txB := utils.CastProtoMessage[*reduced.TotalPaymentValue](b)
// 		return txA.FinalAmount > txB.FinalAmount
// 	}

// 	// Store the transactions in the cache, sorted by FinalAmount
// 	for _, tx := range protoTPV {
// 		tpv := utils.CastProtoMessage[*reduced.TotalPaymentValue](tx)
// 		keyID := tpv.StoreId + "@" + tpv.Semester
// 		err := cacheService.StoreAggregatedData(cacheRef, keyID, tx, joinFn)
// 		assert.NoError(t, err)
// 	}

// 	err := cacheService.SortData(cacheRef, sortFn)
// 	assert.NoError(t, err)

// 	// Read back the sorted transactions
// 	readData, err := cacheService.ReadBatch(cacheRef, int32(len(transactions)))
// 	assert.NoError(t, err)
// 	assert.Equal(t, 3, len(readData))

// 	// Convert readData back into TPV data
// 	tpvResults := make([]*reduced.TotalPaymentValue, len(readData))
// 	for i, msg := range readData {
// 		tx, ok := msg.(*reduced.TotalPaymentValue)
// 		assert.True(t, ok)
// 		tpvResults[i] = tx
// 	}

// 	assert.Equal(t, 6000.0, tpvResults[0].FinalAmount)
// 	assert.Equal(t, "store1", tpvResults[0].StoreId)
// 	assert.Equal(t, "H1", tpvResults[0].Semester)

// 	assert.Equal(t, 4000.0, tpvResults[1].FinalAmount)
// 	assert.Equal(t, "store2", tpvResults[1].StoreId)
// 	assert.Equal(t, "H1", tpvResults[1].Semester)

// 	assert.Equal(t, 1000.0, tpvResults[2].FinalAmount)
// 	assert.Equal(t, "store3", tpvResults[2].StoreId)
// 	assert.Equal(t, "H2", tpvResults[2].Semester)

// }
