package cache_test

import (
	"path/filepath"
	"testing"

	cache "github.com/maxogod/distro-tp/src/aggregator/cache/disk_memory"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func tmpCachePath(t *testing.T, name string) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, name)
}

// ===== INPUT DATA =====

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

var TransactionsMap = map[string]*raw.Transaction{
	"1": {
		TransactionId: "1",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   25.0,
		CreatedAt:     "2025-07-01 06:01:00",
	},
	"2": {
		TransactionId: "2",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 06:00:00",
	},
	"3": {
		TransactionId: "3",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   10.0,
		CreatedAt:     "2025-07-01 23:00:00",
	},
	"4": {
		TransactionId: "4",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   10.0,
		CreatedAt:     "2025-07-01 23:00:00",
	},
}

// ===== TESTS =====

func TestDiskMemoryCache_StoreAndReadData(t *testing.T) {
	c := cache.NewDiskMemoryStorage()
	defer c.Close()

	cacheRef := "test"
	defer c.RemoveCache(cacheRef)

	for _, tr := range Transactions {
		// in actual usage, there would be more than one entry per call
		data, _ := proto.Marshal(tr)
		var transactionDataBytes [][]byte
		transactionDataBytes = append(transactionDataBytes, data)
		if err := c.StoreData(cacheRef, transactionDataBytes); err != nil {
			panic(err)
		}
	}

	readCh := make(chan []byte)
	c.ReadData(cacheRef, readCh)
	var readBack []*raw.Transaction
	for bytes := range readCh {
		transactionGot := &raw.Transaction{}
		proto.Unmarshal(bytes, transactionGot)
		readBack = append(readBack, transactionGot)
	}

	assert.Equal(t, len(Transactions), len(readBack), "Number of transactions read back mismatch")

	for i, data := range readBack {
		expectedTr := Transactions[i]
		assert.True(t, proto.Equal(expectedTr, data),
			"Transaction mismatch at index %d", i)
	}

	assert.NoError(t, c.Close())
}
