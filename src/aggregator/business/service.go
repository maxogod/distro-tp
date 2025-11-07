package business

import (
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"google.golang.org/protobuf/proto"
)

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

// storeBatch is a generic helper for storing any proto.Message slice
func storeBatch[T proto.Message](as *aggregatorService, clientID string, data []T) error {
	protoMessages := make([]proto.Message, len(data))
	for i := range data {
		protoMessages[i] = data[i]
	}
	return as.cacheService.StoreBatch(clientID, protoMessages)
}

func storeAggregatedBatch[T proto.Message](as *aggregatorService, clientID string, dataKey string, data T, joinFn func(existing, new proto.Message) (proto.Message, error)) error {
	return as.cacheService.StoreAggregatedData(clientID, dataKey, data, joinFn)
}

// getBatch is a generic helper for getting any proto.Message type
func getBatch[T proto.Message](as *aggregatorService, clientID string, amount int32) ([]T, bool) {
	protoMessages, err := as.cacheService.ReadBatch(clientID, amount)
	if err != nil {
		logger.Logger.Errorf("Error reading batch from cache: %v", err)
		return nil, false
	}

	if len(protoMessages) == 0 {
		return nil, false
	}

	result := make([]T, len(protoMessages))
	for i, msg := range protoMessages {
		result[i] = utils.CastProtoMessage[T](msg)
	}

	return result, true
}

// ======= STORAGE FUNCTIONS =======

func (as *aggregatorService) StoreTransactions(clientID string, transactions []*raw.Transaction) error {
	return storeBatch(as, clientID, transactions)
}

func (as *aggregatorService) StoreTotalProfitBySubtotal(clientID string, reducedData *reduced.TotalProfitBySubtotal) error {
	key := reducedData.ItemId + SEPERATOR + reducedData.YearMonth
	joinFn := func(existing, new proto.Message) (proto.Message, error) {
		existingData := utils.CastProtoMessage[*reduced.TotalProfitBySubtotal](existing)
		newData := utils.CastProtoMessage[*reduced.TotalProfitBySubtotal](new)
		existingData.Subtotal += newData.Subtotal
		return existing, nil
	}
	return storeAggregatedBatch(as, clientID, key, reducedData, joinFn)
}

func (as *aggregatorService) StoreTotalSoldByQuantity(clientID string, reducedData *reduced.TotalSoldByQuantity) error {

	key := reducedData.ItemId + SEPERATOR + reducedData.YearMonth
	joinFn := func(existing, new proto.Message) (proto.Message, error) {
		existingData := utils.CastProtoMessage[*reduced.TotalSoldByQuantity](existing)
		newData := utils.CastProtoMessage[*reduced.TotalSoldByQuantity](new)
		existingData.Quantity += newData.Quantity
		return existing, nil
	}
	return storeAggregatedBatch(as, clientID, key, reducedData, joinFn)
}

func (as *aggregatorService) StoreTotalPaymentValue(clientID string, reducedData *reduced.TotalPaymentValue) error {
	key := reducedData.StoreId + SEPERATOR + reducedData.Semester
	joinFn := func(existing, new proto.Message) (proto.Message, error) {
		existingData := utils.CastProtoMessage[*reduced.TotalPaymentValue](existing)
		newData := utils.CastProtoMessage[*reduced.TotalPaymentValue](new)
		existingData.FinalAmount += newData.FinalAmount
		return existing, nil
	}
	return storeAggregatedBatch(as, clientID, key, reducedData, joinFn)
}

func (as *aggregatorService) StoreCountedUserTransactions(clientID string, reducedData *reduced.CountedUserTransactions) error {
	key := reducedData.StoreId + SEPERATOR + reducedData.UserId
	joinFn := func(existing, new proto.Message) (proto.Message, error) {
		existingData := utils.CastProtoMessage[*reduced.CountedUserTransactions](existing)
		newData := utils.CastProtoMessage[*reduced.CountedUserTransactions](new)
		existingData.TransactionQuantity += newData.TransactionQuantity
		return existing, nil
	}
	return storeAggregatedBatch(as, clientID, key, reducedData, joinFn)
}

// ======= RETRIEVAL FUNCTIONS =======

func (as *aggregatorService) GetStoredTransactions(clientID string, amount int32) ([]*raw.Transaction, bool) {
	return getBatch[*raw.Transaction](as, clientID, amount)
}

func (as *aggregatorService) GetStoredTotalProfitBySubtotal(clientID string, amount int32) ([]*reduced.TotalProfitBySubtotal, bool) {
	return getBatch[*reduced.TotalProfitBySubtotal](as, clientID, amount)
}

func (as *aggregatorService) GetStoredTotalSoldByQuantity(clientID string, amount int32) ([]*reduced.TotalSoldByQuantity, bool) {
	return getBatch[*reduced.TotalSoldByQuantity](as, clientID, amount)
}

func (as *aggregatorService) GetStoredTotalPaymentValue(clientID string, amount int32) ([]*reduced.TotalPaymentValue, bool) {
	return getBatch[*reduced.TotalPaymentValue](as, clientID, amount)
}

func (as *aggregatorService) GetStoredCountedUserTransactions(clientID string, amount int32) ([]*reduced.CountedUserTransactions, bool) {
	return getBatch[*reduced.CountedUserTransactions](as, clientID, amount)
}

// ======= SORT DATA =======

func (as *aggregatorService) SortData(clientID string, sortFn func(a, b proto.Message) bool) error {
	return as.cacheService.SortData(clientID, sortFn)
}

// ======= CLOSE =======

func (as *aggregatorService) Close() error {
	return as.cacheService.Close()
}
