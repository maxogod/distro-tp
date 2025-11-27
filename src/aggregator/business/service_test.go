package business_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/aggregator/business"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	storage "github.com/maxogod/distro-tp/src/common/worker/storage"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

// ====== INPUT DATA ======

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

func getDataEnvelope(bytes []byte) *protocol.DataEnvelope {
	var dataEnvelopes = protocol.DataEnvelope{
		Payload: bytes,
	}
	return &dataEnvelopes
}

func TestAggregatorService_StoreAggregatedAndReadTotalSoldQuantity(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)

	clientID := "client2"
	c := storage.NewDiskMemoryStorage()
	service := business.NewAggregatorService(c)
	//defer service.RemoveData(clientID)

	// we store 1 data envelope with all the data
	var dataEnvelope *protocol.DataEnvelope
	totalSumItemsBatch := &reduced.TotalSumItemsBatch{
		TotalSumItems: TotalItemSumData,
	}
	bytes, err := proto.Marshal(totalSumItemsBatch)
	assert.NoError(t, err)
	dataEnvelope = getDataEnvelope(bytes)

	err = service.StoreData(clientID, dataEnvelope)
	assert.NoError(t, err)

	subtotalResults, quantityResults, err := service.GetStoredTotalItems(clientID)
	assert.NoError(t, err)

	assert.Len(t, subtotalResults, len(TSubtotalExpected))
	assert.Len(t, quantityResults, len(TQuantityExpected))

	for _, result := range subtotalResults {
		logger.Logger.Infof("Result: %+v", result)
		key := result.ItemId + "@" + result.YearMonth
		expectedAmount, ok := TSubtotalExpected[key]
		assert.True(t, ok, "Unexpected key found: %s", key)
		assert.Equal(t, expectedAmount, result.Subtotal,
			"Subtotal mismatch for key %s", key)
	}
	for _, result := range quantityResults {
		logger.Logger.Infof("Result: %+v", result)
		key := result.ItemId + "@" + result.YearMonth
		expectedAmount, ok := TQuantityExpected[key]
		assert.True(t, ok, "Unexpected key found: %s", key)
		assert.Equal(t, expectedAmount, result.Quantity,
			"Quantity mismatch for key %s", key)
	}
}

// func TestAggregatorService_StoreAggregatedAndReadTpvData(t *testing.T) {
// 	logger.InitLogger(logger.LoggerEnvDevelopment)

// 	clientID := "client3"
// 	c := storage.NewDiskMemoryStorage()
// 	service := business.NewAggregatorService(c)
// 	defer service.RemoveData(clientID)

// 	var dataEnvelopes []*protocol.DataEnvelope
// 	for _, tx := range TPVData {
// 		bytes, err := proto.Marshal(tx)
// 		assert.NoError(t, err)
// 		dataEnvelope := getDataEnvelope(bytes)
// 		dataEnvelopes = append(dataEnvelopes, dataEnvelope)
// 	}
// 	err := service.StoreData(clientID, dataEnvelopes)
// 	assert.NoError(t, err)

// 	results, err := service.GetStoredTotalPaymentValue(clientID)
// 	assert.NoError(t, err)

// 	assert.Len(t, results, len(TPVExpected))

// 	for _, result := range results {
// 		key := result.StoreId + "@" + result.Semester
// 		expectedAmount, ok := TPVExpected[key]
// 		assert.True(t, ok, "Unexpected key found: %s", key)
// 		assert.Equal(t, expectedAmount, result.FinalAmount,
// 			"FinalAmount mismatch for key %s", key)
// 	}
// }

// func TestAggregatorService_StoreAggregatedAndReadTotalCountedUserTransactions(t *testing.T) {
// 	logger.InitLogger(logger.LoggerEnvDevelopment)

// 	clientID := "client4"
// 	c := storage.NewDiskMemoryStorage()
// 	service := business.NewAggregatorService(c)
// 	defer service.RemoveData(clientID)

// 	var dataEnvelopes []*protocol.DataEnvelope
// 	for _, tx := range CountedUserTransactionsData {
// 		bytes, err := proto.Marshal(tx)
// 		assert.NoError(t, err)
// 		dataEnvelope := getDataEnvelope(bytes)
// 		dataEnvelopes = append(dataEnvelopes, dataEnvelope)
// 	}

// 	err := service.StoreData(clientID, dataEnvelopes)
// 	assert.NoError(t, err)

// 	results, err := service.GetStoredCountedUserTransactions(clientID)
// 	assert.NoError(t, err)

// 	assert.Len(t, results, len(CUTExpected))

// 	for _, subResult := range results {
// 		assert.Len(t, subResult, 1, "Expected only one entry per store")
// 		result := subResult[0]
// 		key := result.StoreId + "@" + result.UserId
// 		expectedAmount, ok := CUTExpected[key]
// 		assert.True(t, ok, "Unexpected key found: %s", key)
// 		assert.Equal(t, expectedAmount, result.TransactionQuantity,
// 			"TransactionQuantity mismatch for key %s", key)
// 	}
// }

// func TestReadAndWriteInParallel(t *testing.T) {
// 	logger.InitLogger(logger.LoggerEnvDevelopment)

// 	clientID1 := "client5"
// 	clientID2 := "client6"
// 	c := storage.NewDiskMemoryStorage()
// 	service := business.NewAggregatorService(c)
// 	defer service.RemoveData(clientID1)
// 	defer service.RemoveData(clientID2)

// 	// First we store some data
// 	var dataEnvelopes []*protocol.DataEnvelope
// 	for _, tx := range CountedUserTransactionsData {
// 		bytes, err := proto.Marshal(tx)
// 		assert.NoError(t, err)
// 		dataEnvelope := getDataEnvelope(bytes)
// 		dataEnvelopes = append(dataEnvelopes, dataEnvelope)
// 	}
// 	err := service.StoreData(clientID1, dataEnvelopes)
// 	assert.NoError(t, err)

// 	// Now, we read and write in parallel
// 	done := make(chan bool)

// 	var routineError error

// 	// This represents a routine that writes data
// 	go func() {
// 		for {
// 			// we keep writing until signaled to stop
// 			select {
// 			case <-done:
// 				return
// 			default:
// 				// continue
// 			}
// 			routineError = service.StoreData(clientID2, dataEnvelopes)
// 		}
// 	}()

// 	results, err := service.GetStoredCountedUserTransactions(clientID1)
// 	assert.NoError(t, err)
// 	assert.Len(t, results, len(CUTExpected))
// 	// Signal the writing routine to stop
// 	done <- true
// 	_, err = service.GetStoredCountedUserTransactions(clientID2)
// 	assert.NoError(t, err)
// 	assert.NoError(t, routineError)
// }
