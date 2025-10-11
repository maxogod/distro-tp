package business

import (
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

const SEPERATOR = "@"

type aggregatorService struct {
	cacheService cache.CacheService
}

func NewAggregatorService(cacheService cache.CacheService) AggregatorService {
	return &aggregatorService{
		cacheService: cacheService,
	}
}

// ======= GENERIC HELPERS (Private) =======

// Generic helper for storing any proto.Message slice
func storeBatch[T proto.Message](as *aggregatorService, clientID string, data []T) error {
	protoMessages := make([]*proto.Message, len(data))
	for i := range data {
		msg := proto.Message(data[i])
		protoMessages[i] = &msg
	}
	return as.cacheService.StoreBatch(clientID, protoMessages)
}

func storeAggregatedBatch[T proto.Message](as *aggregatorService, clientID string, dataKey string, data T, joinFn func(existing, new *proto.Message) (*proto.Message, error)) error {
	protoMsg := proto.Message(data)
	return as.cacheService.StoreAggregatedData(clientID, dataKey, &protoMsg, joinFn)
}

// Generic helper for getting any proto.Message type
func getBatch[T proto.Message](as *aggregatorService, clientID string, amount int32) ([]T, bool) {
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
func (as *aggregatorService) StoreTransactions(clientID string, transactions []*raw.Transaction) error {
	return storeBatch(as, clientID, transactions)
}

// This is T2_1
func (as *aggregatorService) StoreTotalProfitBySubtotal(clientID string, reducedData *reduced.TotalProfitBySubtotal) error {

	key := reducedData.ItemId + SEPERATOR + reducedData.YearMonth
	joinFn := func(existing, new *proto.Message) (*proto.Message, error) {
		existingData := (*existing).(*reduced.TotalProfitBySubtotal)
		newData := (*new).(*reduced.TotalProfitBySubtotal)
		existingData.Subtotal += newData.Subtotal
		return existing, nil
	}
	return storeAggregatedBatch(as, clientID, key, reducedData, joinFn)
}

// This is T2_2
func (as *aggregatorService) StoreTotalSoldByQuantity(clientID string, reducedData *reduced.TotalSoldByQuantity) error {

	key := reducedData.ItemId + SEPERATOR + reducedData.YearMonth
	joinFn := func(existing, new *proto.Message) (*proto.Message, error) {
		existingData := (*existing).(*reduced.TotalSoldByQuantity)
		newData := (*new).(*reduced.TotalSoldByQuantity)
		existingData.Quantity += newData.Quantity
		return existing, nil
	}
	return storeAggregatedBatch(as, clientID, key, reducedData, joinFn)
}

// This is T3
func (as *aggregatorService) StoreTotalPaymentValue(clientID string, reducedData *reduced.TotalPaymentValue) error {

	key := reducedData.StoreId + SEPERATOR + reducedData.Semester
	joinFn := func(existing, new *proto.Message) (*proto.Message, error) {
		existingData := (*existing).(*reduced.TotalPaymentValue)
		newData := (*new).(*reduced.TotalPaymentValue)
		existingData.FinalAmount += newData.FinalAmount
		return existing, nil
	}
	return storeAggregatedBatch(as, clientID, key, reducedData, joinFn)
}

// This is T4
func (as *aggregatorService) StoreCountedUserTransactions(clientID string, reducedData *reduced.CountedUserTransactions) error {

	key := reducedData.StoreId + SEPERATOR + reducedData.UserId
	joinFn := func(existing, new *proto.Message) (*proto.Message, error) {
		existingData := (*existing).(*reduced.CountedUserTransactions)
		newData := (*new).(*reduced.CountedUserTransactions)
		existingData.TransactionQuantity += newData.TransactionQuantity
		return existing, nil
	}
	return storeAggregatedBatch(as, clientID, key, reducedData, joinFn)
}

// ======= RETRIEVAL FUNCTIONS =======

// This is T1
func (as *aggregatorService) GetStoredTransactions(clientID string, amount int32) ([]*raw.Transaction, bool) {
	return getBatch[*raw.Transaction](as, clientID, amount)
}

// This is T2_1
func (as *aggregatorService) GetStoredTotalProfitBySubtotal(clientID string, amount int32) ([]*reduced.TotalProfitBySubtotal, bool) {
	return getBatch[*reduced.TotalProfitBySubtotal](as, clientID, amount)
}

// This is T2_2
func (as *aggregatorService) GetStoredTotalSoldByQuantity(clientID string, amount int32) ([]*reduced.TotalSoldByQuantity, bool) {
	return getBatch[*reduced.TotalSoldByQuantity](as, clientID, amount)
}

// This is T3
func (as *aggregatorService) GetStoredTotalPaymentValue(clientID string, amount int32) ([]*reduced.TotalPaymentValue, bool) {
	return getBatch[*reduced.TotalPaymentValue](as, clientID, amount)
}

// This is T4
func (as *aggregatorService) GetStoredCountedUserTransactions(clientID string, amount int32) ([]*reduced.CountedUserTransactions, bool) {
	return getBatch[*reduced.CountedUserTransactions](as, clientID, amount)
}

// ======= SORT DATA =======

func (as *aggregatorService) SortData(clientID string, sortFn func(a, b *proto.Message) bool) error {
	return as.cacheService.SortData(clientID, sortFn)
}

// ======= CLOSE =======

func (as *aggregatorService) Close() error {
	return as.cacheService.Close()
}
