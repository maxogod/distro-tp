package cache_test

import (
	"path/filepath"
	"testing"

	"github.com/maxogod/distro-tp/src/aggregator/cache"
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
	c := cache.NewDiskMemoryCache()
	defer c.Close()

	cacheRef := tmpCachePath(t, "test1")
	for _, tr := range Transactions {
		// in actual usage, there would be more than one entry per call
		data, _ := proto.Marshal(tr)
		var transactionDataBytes [][]byte
		transactionDataBytes = append(transactionDataBytes, data)
		if err := c.StoreData(cacheRef, transactionDataBytes); err != nil {
			t.Fatalf("StoreData error: %v", err)
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

func TestDiskMemoryCache_StoreAggregatedData(t *testing.T) {
	c := cache.NewDiskMemoryCache()
	defer c.Close()

	cacheRef := tmpCachePath(t, "test2")

	// write all indexed data

	for _, tr := range Transactions {

		joinFn := func(existing *[]byte) {
			if len(*existing) == 0 {
				data, _ := proto.Marshal(tr)
				*existing = data
				return
			}
			existingTr := &raw.Transaction{}
			proto.Unmarshal(*existing, existingTr)
			existingTr.FinalAmount += tr.FinalAmount
			data, _ := proto.Marshal(existingTr)
			*existing = data
		}
		dataKey := tr.TransactionId
		err := c.StoreAggregatedData(cacheRef, dataKey, joinFn)
		assert.NoError(t, err)
	}

	// Read back and verify
	readCh := make(chan []byte)
	c.ReadData(cacheRef, readCh)
	var results []*raw.Transaction
	for b := range readCh {
		tx := &raw.Transaction{}
		proto.Unmarshal(b, tx)
		results = append(results, tx)
	}

	assert.Len(t, results, 2)
	assert.Equal(t, "1", results[0].TransactionId)
	assert.Equal(t, 200.0, results[0].FinalAmount)
	assert.Equal(t, "2", results[1].TransactionId)
	assert.Equal(t, 100.0, results[1].FinalAmount)
}

func TestDiskMemoryCache_SortData(t *testing.T) {
	c := cache.NewDiskMemoryCache()
	defer c.Close()

	cacheRef := tmpCachePath(t, "test3")

	unsortedTransactionIds := []string{"3", "1", "4", "2"}
	// write all indexed data in unsorted order
	for _, txID := range unsortedTransactionIds {
		tr := TransactionsMap[txID]
		data, _ := proto.Marshal(tr)
		var transactionDataBytes [][]byte
		transactionDataBytes = append(transactionDataBytes, data)
		if err := c.StoreData(cacheRef, transactionDataBytes); err != nil {
			t.Fatalf("StoreData error: %v", err)
		}
	}

	// sort by TransactionId
	sortFn := func(a, b []byte) bool {
		trA := &raw.Transaction{}
		trB := &raw.Transaction{}
		proto.Unmarshal(a, trA)
		proto.Unmarshal(b, trB)
		return trA.TransactionId < trB.TransactionId
	}
	err := c.SortData(cacheRef, sortFn)
	assert.NoError(t, err)

	// Read back and verify
	readCh := make(chan []byte)
	c.ReadData(cacheRef, readCh)
	var results []*raw.Transaction
	for b := range readCh {
		tx := &raw.Transaction{}
		proto.Unmarshal(b, tx)
		results = append(results, tx)
	}

	assert.Len(t, results, len(TransactionsMap))
	sortedOrder := []string{"1", "2", "3", "4"}
	for i, expectedID := range sortedOrder {
		assert.True(t, proto.Equal(TransactionsMap[expectedID], results[i]),
			"Transaction mismatch for ID %s", expectedID)
	}
}
