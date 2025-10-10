package business

import (
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type AggregatorService struct {
	cacheService cache.CacheService
}

func NewAggregatorService(cacheService cache.CacheService) AggregatorService {
	return AggregatorService{
		cacheService: cacheService,
	}
}

// ======= GENERIC HELPERS (Private) =======

// Generic helper for storing any proto.Message slice
func storeBatch[T proto.Message](as *AggregatorService, clientID string, data []T) error {
	protoMessages := make([]*proto.Message, len(data))
	for i := range data {
		msg := proto.Message(data[i])
		protoMessages[i] = &msg
	}
	return as.cacheService.StoreBatch(clientID, protoMessages)
}

// Generic helper for getting any proto.Message type
func getBatch[T proto.Message](as *AggregatorService, clientID string, amount int32) ([]T, bool) {
	protoMessages, err := as.cacheService.ReadBatch(clientID, amount)
	if err != nil {
		log.Errorf("Error reading batch from cache: %v", err)
		return nil, false
	}

	if len(protoMessages) == 0 {
		return nil, false
	}

	result := make([]T, len(protoMessages))
	for i, msg := range protoMessages {
		result[i] = (*msg).(T)
	}

	return result, true
}

// ======= STORAGE FUNCTIONS =======

// This is T1
func (as *AggregatorService) StoreTransactions(clientID string, transactions []*raw.Transaction) error {
	return storeBatch(as, clientID, transactions)
}

// This is T2_1
func (as *AggregatorService) StoreTotalProfitBySubtotal(clientID string, reducedData []*reduced.TotalProfitBySubtotal) error {
	return storeBatch(as, clientID, reducedData)
}

// This is T2_2
func (as *AggregatorService) StoreTotalSoldByQuantity(clientID string, reducedData []*reduced.TotalSoldByQuantity) error {
	return storeBatch(as, clientID, reducedData)
}

// This is T3
func (as *AggregatorService) StoreTotalPaymentValue(clientID string, reducedData []*reduced.TotalPaymentValue) error {
	return storeBatch(as, clientID, reducedData)
}

// This is T4
func (as *AggregatorService) StoreCountedUserTransactions(clientID string, reducedData []*reduced.CountedUserTransactions) error {
	return storeBatch(as, clientID, reducedData)
}

// ======= RETRIEVAL FUNCTIONS =======

// This is T1
func (as *AggregatorService) GetStoredTransactions(clientID string, amount int32) ([]*raw.Transaction, bool) {
	return getBatch[*raw.Transaction](as, clientID, amount)
}

// This is T2_1
func (as *AggregatorService) GetStoredTotalProfitBySubtotal(clientID string, amount int32) ([]*reduced.TotalProfitBySubtotal, bool) {
	return getBatch[*reduced.TotalProfitBySubtotal](as, clientID, amount)
}

// This is T2_2
func (as *AggregatorService) GetStoredTotalSoldByQuantity(clientID string, amount int32) ([]*reduced.TotalSoldByQuantity, bool) {
	return getBatch[*reduced.TotalSoldByQuantity](as, clientID, amount)
}

// This is T3
func (as *AggregatorService) GetStoredTotalPaymentValue(clientID string, amount int32) ([]*reduced.TotalPaymentValue, bool) {
	return getBatch[*reduced.TotalPaymentValue](as, clientID, amount)
}

// This is T4
func (as *AggregatorService) GetStoredCountedUserTransactions(clientID string, amount int32) ([]*reduced.CountedUserTransactions, bool) {
	return getBatch[*reduced.CountedUserTransactions](as, clientID, amount)
}

// ======= CLOSE =======

func (as *AggregatorService) Close() error {
	return as.cacheService.Close()
}
