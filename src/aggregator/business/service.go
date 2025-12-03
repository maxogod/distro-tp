package business

import (
	"sort"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/common/worker/storage"
	"google.golang.org/protobuf/proto"
)

const SEPERATOR = "#"

type aggregatorService struct {
	storageService storage.StorageService
}

func NewAggregatorService(storageService storage.StorageService) AggregatorService {
	as := &aggregatorService{
		storageService: storageService,
	}
	return as
}

// ======= STORAGE FUNCTIONS =======

func (as *aggregatorService) StoreData(clientID string, data *protocol.DataEnvelope) error {
	return storage.StoreTempBatch(as.storageService, clientID, []*protocol.DataEnvelope{data})
}

func (as *aggregatorService) ConfirmStorage(clientID string) error {
	return as.storageService.SaveTempFile(clientID)
}

// ======= RETRIEVAL FUNCTIONS =======

func (as *aggregatorService) GetStoredTotalItems(clientID string) ([]*reduced.TotalSumItem, []*reduced.TotalSumItem, error) {
	as.storageService.StopWriting(clientID)

	unwrapper := func(dataEnvelopes []*protocol.DataEnvelope) []*reduced.TotalSumItem {
		results := make([]*reduced.TotalSumItem, 0)
		for _, dataEnvelope := range dataEnvelopes {
			data := &reduced.TotalSumItemsBatch{}
			err := proto.Unmarshal(dataEnvelope.GetPayload(), data)
			if err != nil {
				logger.Logger.Errorf("Error unmarshalling TotalSumItemBatch: %v", err)
				continue
			}
			results = append(results, data.GetTotalSumItems()...)
		}
		return results
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

	data, err := getData(as, clientID, unwrapper, joinFn)
	if err != nil {
		return nil, nil, err
	}

	bestBySubtotal := filterBestMonthValues(
		data,
		func(t *reduced.TotalSumItem) string { return t.GetYearMonth() },
		func(t *reduced.TotalSumItem) float64 { return t.GetSubtotal() },
	)

	bestByQuantity := filterBestMonthValues(
		data,
		func(t *reduced.TotalSumItem) string { return t.GetYearMonth() },
		func(t *reduced.TotalSumItem) float64 { return float64(t.GetQuantity()) },
	)

	return bestBySubtotal, bestByQuantity, nil
}

func (as *aggregatorService) GetStoredTotalPaymentValue(clientID string) ([]*reduced.TotalPaymentValue, error) {
	as.storageService.StopWriting(clientID)

	unwrapper := func(dataEnvelopes []*protocol.DataEnvelope) []*reduced.TotalPaymentValue {
		results := make([]*reduced.TotalPaymentValue, 0)
		for _, dataEnvelope := range dataEnvelopes {
			data := &reduced.TotalPaymentValueBatch{}
			err := proto.Unmarshal(dataEnvelope.GetPayload(), data)
			if err != nil {
				logger.Logger.Errorf("Error unmarshalling TotalPaymentValueBatch: %v", err)
				continue
			}
			results = append(results, data.GetTotalPaymentValues()...)
		}
		return results
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
	data, err := getData(as, clientID, unwrapper, joinFn)
	sortData(data, sortFn)
	return data, err
}

func (as *aggregatorService) GetStoredCountedUserTransactions(clientID string) (map[string][]*reduced.CountedUserTransactions, error) {
	as.storageService.StopWriting(clientID)

	unwrapper := func(dataEnvelopes []*protocol.DataEnvelope) []*reduced.CountedUserTransactions {
		results := make([]*reduced.CountedUserTransactions, 0)
		for _, dataEnvelope := range dataEnvelopes {
			data := &reduced.CountedUserTransactionBatch{}
			err := proto.Unmarshal(dataEnvelope.GetPayload(), data)
			if err != nil {
				logger.Logger.Errorf("Error unmarshalling CountedUserTransactionBatch: %v", err)
				continue
			}
			results = append(results, data.GetCountedUserTransactions()...)
		}
		return results
	}

	joinFn := func(newData *reduced.CountedUserTransactions, flattenedDataMap map[string]*reduced.CountedUserTransactions) {
		key := newData.StoreId + SEPERATOR + newData.UserId
		if existingData, exists := flattenedDataMap[key]; exists {
			existingData.TransactionQuantity += newData.TransactionQuantity
		} else {
			flattenedDataMap[key] = newData
		}
	}

	data, err := getData(as, clientID, unwrapper, joinFn)

	if err != nil {
		return nil, err
	}
	return getTopUsersPerStore(data), nil
}

// ======= CLOSE =======

func (as *aggregatorService) Close() error {
	return as.storageService.Close()
}

func (as *aggregatorService) RemoveData(clientID string) error {
	return as.storageService.RemoveCache(clientID)
}

// ======= GENERIC HELPERS (Private) =======

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

// getDataEnvelopes retrieves all unique DataEnvelopes for a given clientID.
func (as *aggregatorService) getDataEnvelopes(clientID string) ([]*protocol.DataEnvelope, error) {

	readCh, err := as.storageService.ReadAllData(clientID)
	if err != nil {
		return nil, err
	}

	uniqueDataEnvelopes := make(map[*protocol.DataEnvelope]bool)
	var result []*protocol.DataEnvelope
	for protoBytes := range readCh {
		dataEnvelope := &protocol.DataEnvelope{}
		err := proto.Unmarshal(protoBytes, dataEnvelope)
		if err != nil {
			logger.Logger.Errorf("Error unmarshalling DataEnvelope: %v", err)
			continue
		}
		if _, exists := uniqueDataEnvelopes[dataEnvelope]; !exists {
			uniqueDataEnvelopes[dataEnvelope] = true
		}
	}
	for dataEnvelope := range uniqueDataEnvelopes {
		result = append(result, dataEnvelope)
	}
	return result, nil
}

func getData[T proto.Message](as *aggregatorService, clientID string, unwrapper func([]*protocol.DataEnvelope) []T, joinFn func(T, map[string]T)) ([]T, error) {

	envelopes, err := as.getDataEnvelopes(clientID)
	if err != nil {
		return nil, err
	}
	rawData := unwrapper(envelopes)

	flattenedDataMap := make(map[string]T)
	var result []T

	// Now process each unique DataEnvelope
	for _, protoData := range rawData {
		if joinFn != nil {
			joinFn(protoData, flattenedDataMap)
		} else {
			result = append(result, protoData)
		}
	}

	if joinFn != nil {
		for _, data := range flattenedDataMap {
			result = append(result, data)
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
