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
	cacheService  cache.CacheService
	read_channels map[string]chan []byte
}

func NewAggregatorService(cacheService cache.CacheService) AggregatorService {
	return &aggregatorService{
		cacheService:  cacheService,
		read_channels: make(map[string]chan []byte),
	}
}

// ======= GENERIC HELPERS (Private) =======

// storeBatch is a generic helper for storing any proto.Message slice
func storeBatch[T proto.Message](as *aggregatorService, clientID string, data []T) error {
	listBytes := make([][]byte, len(data))
	for i := range data {
		bytes, err := proto.Marshal(data[i])
		if err != nil {
			log.Errorf("Error marshalling proto message: %v", err)
			return err
		}
		listBytes[i] = bytes
	}
	return as.cacheService.StoreData(clientID, listBytes)
}

// getBatch is a generic helper for getting any proto.Message type
func getBatch[T proto.Message](as *aggregatorService, clientID string, amount int32, factory func() T) ([]T, bool) {

	read_ch, exists := as.read_channels[clientID]
	if !exists {
		read_ch = make(chan []byte)
		as.cacheService.ReadData(clientID, read_ch)
		as.read_channels[clientID] = read_ch
	}

	var result []T
	for protoBytes := range read_ch {
		protoData := factory()
		err := proto.Unmarshal(protoBytes, protoData)
		if err != nil {
			log.Errorf("Error unmarshalling proto message: %v", err)
			return nil, false
		}
		result = append(result, protoData)

		if int32(len(result)) >= amount {
			break
		}
	}

	return result, true
}

// ======= STORAGE FUNCTIONS =======

func (as *aggregatorService) StoreTransactions(clientID string, transactions []*raw.Transaction) error {
	return storeBatch(as, clientID, transactions)
}

func (as *aggregatorService) StoreTotalProfitBySubtotal(clientID string, reducedData *reduced.TotalProfitBySubtotal) error {
	dataKey := reducedData.ItemId + reducedData.YearMonth
	joinFn := func(existingBytes *[]byte) {
		if len(*existingBytes) == 0 {
			data, _ := proto.Marshal(reducedData)
			*existingBytes = data
			return
		}
		existingData := &reduced.TotalProfitBySubtotal{}
		proto.Unmarshal(*existingBytes, existingData)
		existingData.Subtotal += reducedData.Subtotal
		data, _ := proto.Marshal(existingData)
		*existingBytes = data
	}
	return as.cacheService.StoreAggregatedData(clientID, dataKey, joinFn)
}

func (as *aggregatorService) StoreTotalSoldByQuantity(clientID string, reducedData *reduced.TotalSoldByQuantity) error {

	dataKey := reducedData.ItemId + reducedData.YearMonth
	joinFn := func(existingBytes *[]byte) {
		if len(*existingBytes) == 0 {
			data, _ := proto.Marshal(reducedData)
			*existingBytes = data
			return
		}
		existingData := &reduced.TotalSoldByQuantity{}
		proto.Unmarshal(*existingBytes, existingData)
		existingData.Quantity += reducedData.Quantity
		data, _ := proto.Marshal(existingData)
		*existingBytes = data
	}
	return as.cacheService.StoreAggregatedData(clientID, dataKey, joinFn)
}

func (as *aggregatorService) StoreTotalPaymentValue(clientID string, reducedData *reduced.TotalPaymentValue) error {
	dataKey := reducedData.StoreId + reducedData.Semester
	joinFn := func(existingBytes *[]byte) {
		if len(*existingBytes) == 0 {
			data, _ := proto.Marshal(reducedData)
			*existingBytes = data
			return
		}
		existingData := &reduced.TotalPaymentValue{}
		proto.Unmarshal(*existingBytes, existingData)
		existingData.FinalAmount += reducedData.FinalAmount
		data, _ := proto.Marshal(existingData)
		*existingBytes = data
	}
	return as.cacheService.StoreAggregatedData(clientID, dataKey, joinFn)
}

func (as *aggregatorService) StoreCountedUserTransactions(clientID string, reducedData *reduced.CountedUserTransactions) error {
	dataKey := reducedData.StoreId + reducedData.UserId
	joinFn := func(existingBytes *[]byte) {
		if len(*existingBytes) == 0 {
			data, _ := proto.Marshal(reducedData)
			*existingBytes = data
			return
		}
		existingData := &reduced.CountedUserTransactions{}
		proto.Unmarshal(*existingBytes, existingData)
		existingData.TransactionQuantity += reducedData.TransactionQuantity
		data, _ := proto.Marshal(existingData)
		*existingBytes = data
	}
	return as.cacheService.StoreAggregatedData(clientID, dataKey, joinFn)
}

// ======= RETRIEVAL FUNCTIONS =======

func (as *aggregatorService) GetStoredTransactions(clientID string, amount int32) ([]*raw.Transaction, bool) {
	return getBatch(as, clientID, amount, func() *raw.Transaction {
		return &raw.Transaction{}
	})
}

func (as *aggregatorService) GetStoredTotalProfitBySubtotal(clientID string, amount int32) ([]*reduced.TotalProfitBySubtotal, bool) {
	return getBatch(as, clientID, amount, func() *reduced.TotalProfitBySubtotal {
		return &reduced.TotalProfitBySubtotal{}
	})
}

func (as *aggregatorService) GetStoredTotalSoldByQuantity(clientID string, amount int32) ([]*reduced.TotalSoldByQuantity, bool) {
	return getBatch(as, clientID, amount, func() *reduced.TotalSoldByQuantity {
		return &reduced.TotalSoldByQuantity{}
	})
}

func (as *aggregatorService) GetStoredTotalPaymentValue(clientID string, amount int32) ([]*reduced.TotalPaymentValue, bool) {
	return getBatch(as, clientID, amount, func() *reduced.TotalPaymentValue {
		return &reduced.TotalPaymentValue{}
	})
}

func (as *aggregatorService) GetStoredCountedUserTransactions(clientID string, amount int32) ([]*reduced.CountedUserTransactions, bool) {
	return getBatch(as, clientID, amount, func() *reduced.CountedUserTransactions {
		return &reduced.CountedUserTransactions{}
	})
}

// ======= SORT DATA =======

func (as *aggregatorService) SortData(clientID string, sortFn func(a, b []byte) bool) error {
	return as.cacheService.SortData(clientID, sortFn)
}

// ======= CLOSE =======

func (as *aggregatorService) Close() error {
	return as.cacheService.Close()
}

func (as *aggregatorService) FinishData(clientID string) error {
	return as.cacheService.RemoveCache(clientID)
}
