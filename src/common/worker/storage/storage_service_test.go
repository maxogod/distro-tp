package storage_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/worker/storage"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

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
	logger.InitLogger(logger.LoggerEnvDevelopment)
	c := storage.NewDiskMemoryStorage()
	cacheRef := "test"
	defer c.RemoveCache(cacheRef)

	for _, tr := range Transactions {
		data, _ := proto.Marshal(tr)
		var transactionDataBytes [][]byte
		transactionDataBytes = append(transactionDataBytes, data)
		if err := c.StartWriting(cacheRef, transactionDataBytes); err != nil {
			panic(err)
		}
	}
	c.StopWriting(cacheRef)

	readCh, err := c.ReadAllData(cacheRef)
	if err != nil {
		panic(err)
	}
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

func TestDiskMemoryCache_ReadFromSameFileInMultipleRoutines(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	c := storage.NewDiskMemoryStorage()
	cacheRef := "test"
	defer c.RemoveCache(cacheRef)

	for _, tr := range Transactions {
		data, _ := proto.Marshal(tr)
		var transactionDataBytes [][]byte
		transactionDataBytes = append(transactionDataBytes, data)
		if err := c.StartWriting(cacheRef, transactionDataBytes); err != nil {
			panic(err)
		}
	}
	c.StopWriting(cacheRef)

	amountOfReaders := 3

	resultsCh := make(chan int)

	for range amountOfReaders {
		go func() {
			readCh, err := c.ReadAllData(cacheRef)
			if err != nil {
				panic(err)
			}
			readCounter := 0
			for bytes := range readCh {
				transactionGot := &raw.Transaction{}
				proto.Unmarshal(bytes, transactionGot)
				readCounter++
			}
			resultsCh <- readCounter
		}()
	}

	for result := range resultsCh {
		assert.Equal(t, len(Transactions), result, "Number of transactions read back mismatch")
		amountOfReaders--
		if amountOfReaders == 0 {
			close(resultsCh)
		}
	}

	assert.NoError(t, c.Close())
}

func TestDiskMemoryCache_ReadUpTo(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	c := storage.NewDiskMemoryStorage()
	cacheRef := "test"
	defer c.RemoveCache(cacheRef)

	items := 100000
	count := 0
	var writeData atomic.Bool
	writeData.Store(true)

	go func() { // Simulate concurrent writes
		for writeData.Load() {
			count++
			i := count % items
			tr := Transactions[i%len(Transactions)]
			data, _ := proto.Marshal(tr)
			var transactionDataBytes [][]byte
			transactionDataBytes = append(transactionDataBytes, data)
			if err := c.StartWriting(cacheRef, transactionDataBytes); err != nil {
				panic(err)
			}
		}
	}()

	time.Sleep(1 * time.Second) // Let some writes happen

	readCh, err := c.ReadData(cacheRef)
	if err != nil {
		panic(err)
	}
	var readBack []*raw.Transaction
	for bytes := range readCh {
		transactionGot := &raw.Transaction{}
		proto.Unmarshal(bytes, transactionGot)
		readBack = append(readBack, transactionGot)
	}

	writeData.Store(false) // Stop writing
	t.Log("Amount Read: ", len(readBack))
	assert.NotEqual(t, 0, len(readBack), "The amout of read items should be less than stored items")

}

func TestDiskMemoryCache_TempFileStorage(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	c := storage.NewDiskMemoryStorage()
	cacheRef := "test"
	defer c.RemoveCache(cacheRef)

	storage.StoreTempBatch(c, cacheRef, Transactions)
	assert.FileExists(t, "storage/TEMP%test.cache", "test_temp file should exist in the current directory")

	savedRef := c.SaveTempFile(cacheRef)
	assert.Equal(t, cacheRef, savedRef, "Saved cache reference should match original")

	assert.NoError(t, c.Close())
}
