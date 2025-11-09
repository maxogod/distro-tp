package business

import (
	"sort"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/common/worker/cache"
	"google.golang.org/protobuf/proto"
)

const SEPERATOR = "#"

type aggregatorService struct {
	cacheService  cache.StorageService
	done_channels map[string]chan string
}

func NewAggregatorService(cacheService cache.StorageService) AggregatorService {
	as := &aggregatorService{
		cacheService:  cacheService,
		done_channels: make(map[string]chan string),
	}
	return as
}

// ======= GENERIC HELPERS (Private) =======

// storeBatch is a generic helper for storing any proto.Message slice
func storeBatch[T proto.Message](as *aggregatorService, clientID string, data []T) error {
	listBytes := make([][]byte, len(data))
	for i := range data {
		bytes, err := proto.Marshal(data[i])
		if err != nil {
			logger.Logger.Errorf("Error marshalling proto message: %v", err)
			return err
		}
		listBytes[i] = bytes
	}
	err := as.cacheService.StoreData(clientID, listBytes)
	if err != nil {
		logger.Logger.Errorf("Error storing data for client [%s]: %v", clientID, err)
	}
	return nil
}

func filterBestMonthValues[T proto.Message](
	data []T,
	yearMonthFn func(T) string,
	valueFn func(T) float64,
) []T {
	bestMonthMap := make(map[string]T)
	for _, item := range data {
		yearMonth := yearMonthFn(item)
		value := valueFn(item)
		if existing, exists := bestMonthMap[yearMonth]; !exists || value > valueFn(existing) {
			bestMonthMap[yearMonth] = item
		}
	}

	sortedKeys := make([]string, 0, len(bestMonthMap))

	for yearMonth := range bestMonthMap {
		sortedKeys = append(sortedKeys, yearMonth)
	}
	sort.Strings(sortedKeys)

	results := make([]T, 0, len(bestMonthMap))
	for _, yearMonth := range sortedKeys {
		bestMonth := bestMonthMap[yearMonth]
		results = append(results, bestMonth)
	}

	return results
}

func getData[T proto.Message](as *aggregatorService, clientID string, factory func() T, joinFn func(T, map[string]T)) ([]T, error) {

	read_ch := make(chan []byte)
	as.cacheService.ReadData(clientID, read_ch)
	flattenedDataMap := make(map[string]T)

	var result []T

	for protoBytes := range read_ch {
		protoData := factory()
		err := proto.Unmarshal(protoBytes, protoData)
		if err != nil {
			logger.Logger.Errorf("Error unmarshalling proto message: %v", err)
			return nil, err
		}
		if joinFn != nil {
			joinFn(protoData, flattenedDataMap)
		} else {
			result = append(result, protoData)
		}

	}

	if joinFn != nil {
		for _, v := range flattenedDataMap {
			result = append(result, v)
		}
	}

	return result, nil
}

func sortData[T proto.Message](data []T, sortFn func(a, b T) bool) {
	if data == nil {
		return
	}
	sort.Slice(data, func(i, j int) bool {
		return sortFn(data[i], data[j])
	})
}

func getTopUsersPerStore(countedUserTransactions []*reduced.CountedUserTransactions) map[string][]*reduced.CountedUserTransactions {
	topUsersPerStorePerQuantity := make(map[string]map[int32][]*reduced.CountedUserTransactions)
	topUsersPerStore := make(map[string][]*reduced.CountedUserTransactions)

	for _, countUserTrans := range countedUserTransactions {
		storeID := countUserTrans.GetStoreId()
		quantity := countUserTrans.GetTransactionQuantity()

		if _, exists := topUsersPerStorePerQuantity[storeID]; !exists {
			topUsersPerStorePerQuantity[storeID] = make(map[int32][]*reduced.CountedUserTransactions)
		}
		// Check if the quantity exists in the second hashmap
		if _, exists := topUsersPerStorePerQuantity[storeID][quantity]; !exists {
			topUsersPerStorePerQuantity[storeID][quantity] = []*reduced.CountedUserTransactions{}
		}
		topUsersPerStorePerQuantity[storeID][quantity] = append(topUsersPerStorePerQuantity[storeID][quantity], countUserTrans)
	}
	for storeID, quantityMap := range topUsersPerStorePerQuantity {
		// Step 1: Sort the keys (quantities) in descending order
		sortedQuantities := make([]int32, 0, len(quantityMap))
		for quantity := range quantityMap {
			sortedQuantities = append(sortedQuantities, quantity)
		}
		sort.Slice(sortedQuantities, func(i, j int) bool {
			return sortedQuantities[i] > sortedQuantities[j]
		})
		// Step 2: Flatten the lists in the order of sorted keys
		var orderedList []*reduced.CountedUserTransactions
		for _, quantity := range sortedQuantities {
			orderedList = append(orderedList, quantityMap[quantity]...)
		}
		topUsersPerStore[storeID] = append(topUsersPerStore[storeID], orderedList...)
	}

	return topUsersPerStore
}

// ======= STORAGE FUNCTIONS =======

func (as *aggregatorService) StoreTransactions(clientID string, transactions []*raw.Transaction) error {
	return storeBatch(as, clientID, transactions)
}

func (as *aggregatorService) StoreTotalItems(clientID string, reducedData *reduced.TotalSumItem) error {
	return storeBatch(as, clientID, []*reduced.TotalSumItem{reducedData})
}

func (as *aggregatorService) StoreTotalPaymentValue(clientID string, reducedData *reduced.TotalPaymentValue) error {
	return storeBatch(as, clientID, []*reduced.TotalPaymentValue{reducedData})
}

func (as *aggregatorService) StoreCountedUserTransactions(clientID string, reducedData *reduced.CountedUserTransactions) error {
	return storeBatch(as, clientID, []*reduced.CountedUserTransactions{reducedData})
}

// ======= RETRIEVAL FUNCTIONS =======

func (as *aggregatorService) GetStoredTransactions(clientID string) ([]*raw.Transaction, error) {
	factory := func() *raw.Transaction {
		return &raw.Transaction{}
	}
	return getData(as, clientID, factory, nil)
}

func (as *aggregatorService) GetStoredTotalItems(clientID string) ([]*reduced.TotalSumItem, []*reduced.TotalSumItem, error) {

	factory := func() *reduced.TotalSumItem {
		return &reduced.TotalSumItem{}
	}

	joinFn := func(newData *reduced.TotalSumItem, flattenedDataMap map[string]*reduced.TotalSumItem) {
		key := newData.ItemId + SEPERATOR + newData.YearMonth
		if existingData, exists := flattenedDataMap[key]; exists {
			existingData.Subtotal += newData.Subtotal
			existingData.Quantity += newData.Quantity
		} else {
			flattenedDataMap[key] = newData
		}
	}

	data, err := getData(as, clientID, factory, joinFn)
	if err != nil {
		return nil, nil, err
	}

	bestBySubtotal := filterBestMonthValues(
		data,
		func(t *reduced.TotalSumItem) string { return t.GetYearMonth() },
		func(t *reduced.TotalSumItem) float64 { return float64(t.GetSubtotal()) },
	)

	bestByQuantity := filterBestMonthValues(
		data,
		func(t *reduced.TotalSumItem) string { return t.GetYearMonth() },
		func(t *reduced.TotalSumItem) float64 { return float64(t.GetQuantity()) },
	)

	return bestBySubtotal, bestByQuantity, nil
}

func (as *aggregatorService) GetStoredTotalPaymentValue(clientID string) ([]*reduced.TotalPaymentValue, error) {

	factory := func() *reduced.TotalPaymentValue {
		return &reduced.TotalPaymentValue{}
	}

	joinFn := func(newData *reduced.TotalPaymentValue, flattenedDataMap map[string]*reduced.TotalPaymentValue) {
		key := newData.StoreId + SEPERATOR + newData.Semester
		if existingData, exists := flattenedDataMap[key]; exists {
			existingData.FinalAmount += newData.FinalAmount
		} else {
			flattenedDataMap[key] = newData
		}
	}

	sortFn := func(a, b *reduced.TotalPaymentValue) bool {

		yearA, halfA := utils.ParseSemester(a.GetSemester())
		yearB, halfB := utils.ParseSemester(b.GetSemester())
		// Compare by year first, then by half-year
		if yearA != yearB {
			// Older years come first
			return yearA < yearB
		}
		// H1 comes before H2
		return halfA < halfB
	}
	data, err := getData(as, clientID, factory, joinFn)
	sortData(data, sortFn)
	return data, err
}

func (as *aggregatorService) GetStoredCountedUserTransactions(clientID string) (map[string][]*reduced.CountedUserTransactions, error) {

	factory := func() *reduced.CountedUserTransactions {
		return &reduced.CountedUserTransactions{}
	}

	joinFn := func(newData *reduced.CountedUserTransactions, flattenedDataMap map[string]*reduced.CountedUserTransactions) {
		key := newData.StoreId + SEPERATOR + newData.UserId
		if existingData, exists := flattenedDataMap[key]; exists {
			existingData.TransactionQuantity += newData.TransactionQuantity
		} else {
			flattenedDataMap[key] = newData
		}
	}

	data, err := getData(as, clientID, factory, joinFn)

	if err != nil {
		return nil, err
	}
	return getTopUsersPerStore(data), nil
}

// ======= CLOSE =======

func (as *aggregatorService) Close() error {
	return as.cacheService.Close()
}

func (as *aggregatorService) RemoveData(clientID string) error {
	return as.cacheService.RemoveCache(clientID)
}
