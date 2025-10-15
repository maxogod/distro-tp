package business

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

const SEPERATOR = "@"
const MENU_ITEM = "menu_item"
const STORE = "store"
const USER = "user"

// TODO: [1]
// This implementation only works asusming that all of the reference data is sent before all of the reduced data.
// If this is not the case, we would need to implement a more complex buffering mechanism.
// One possible approach is to buffer reduced data and only once the finish message is received,
// we can start processing the buffered data for that client.

// TODO: [2]
// This code is almost 300 lines long, and a lot of it is repetitive.
// Apply DRY principle to reduce code duplication.
type joinerService struct {
	cacheService   cache.CacheService
	fullRefClients map[string]bool // Used as a set
}

func NewJoinerService(cacheService cache.CacheService) JoinerService {
	return &joinerService{
		cacheService:   cacheService,
		fullRefClients: make(map[string]bool),
	}
}

/* --- Store raw reference data --- */

func storeRefData[T proto.Message](clientID string, items []T, getRefID func(T) string, cacheService cache.CacheService) error {
	for _, item := range items {
		referenceID := getRefID(item)
		err := cacheService.StoreRefData(clientID, referenceID, item)
		if err != nil {
			log.Errorf("Failed to store ref data %s for client %s: %v", referenceID, clientID, err)
			return err
		}
	}
	return nil
}

// Usage in joinerService methods:
func (js *joinerService) StoreMenuItems(clientID string, items []*raw.MenuItem) error {
	return storeRefData(clientID, items, func(item *raw.MenuItem) string {
		return item.ItemId + SEPERATOR + MENU_ITEM
	}, js.cacheService)
}

func (js *joinerService) StoreShops(clientID string, items []*raw.Store) error {
	return storeRefData(clientID, items, func(item *raw.Store) string {
		return item.StoreId + SEPERATOR + STORE
	}, js.cacheService)
}

func (js *joinerService) StoreUsers(clientID string, items []*raw.User) error {
	return storeRefData(clientID, items, func(item *raw.User) string {
		return item.UserId + SEPERATOR + USER
	}, js.cacheService)
}

func (js *joinerService) FinishStoringRefData(clientID string) error {
	log.Debug("Received all reference data for client: ", clientID)
	js.fullRefClients[clientID] = true // All reference data was received for this client
	return nil
}

/* --- Get joined data --- */

// This is T2_1
func (js *joinerService) JoinTotalProfitBySubtotal(profit *reduced.TotalProfitBySubtotal, clientID string) []*reduced.TotalProfitBySubtotal {
	bufferID := "T2_1" + SEPERATOR + clientID
	referenceID := profit.GetItemId() + SEPERATOR + MENU_ITEM

	_, allRefPresent := js.fullRefClients[clientID]
	if !allRefPresent {
		js.cacheService.BufferUnreferencedData(clientID, bufferID, profit)
		return nil
	}
	protoRef, err := js.cacheService.GetRefData(clientID, referenceID)
	if err != nil {
		log.Errorf("Error retrieving reference data %s for client %s: %v", referenceID, clientID, err)
		return nil
	}
	joinedData := make([]*reduced.TotalProfitBySubtotal, 0)
	menuItem := utils.CastProtoMessage[*raw.MenuItem](protoRef)
	profit.ItemId = menuItem.GetItemName()
	joinedData = append(joinedData, profit)
	// In case there are buffered profits waiting for this reference, resolve them now
	js.joinBufferedProfitData(clientID, bufferID, &joinedData)
	return joinedData
}

// This is T2_2
func (js *joinerService) JoinTotalSoldByQuantity(sales *reduced.TotalSoldByQuantity, clientID string) []*reduced.TotalSoldByQuantity {
	bufferID := "T2_2" + SEPERATOR + clientID
	referenceID := sales.GetItemId() + SEPERATOR + MENU_ITEM
	_, allRefPresent := js.fullRefClients[clientID]
	if !allRefPresent {
		js.cacheService.BufferUnreferencedData(clientID, bufferID, sales)
		return nil
	}
	protoRef, err := js.cacheService.GetRefData(clientID, referenceID)
	if err != nil {
		log.Errorf("Error retrieving reference data %s for client %s: %v", referenceID, clientID, err)
		return nil
	}
	joinedData := make([]*reduced.TotalSoldByQuantity, 0)
	menuItem := utils.CastProtoMessage[*raw.MenuItem](protoRef)
	sales.ItemId = menuItem.GetItemName()
	joinedData = append(joinedData, sales)
	// In case there are buffered profits waiting for this reference, resolve them now
	js.joinBufferedSalesData(clientID, bufferID, &joinedData)
	return joinedData
}

// This is T3
func (js *joinerService) JoinTotalPaymentValue(tpv *reduced.TotalPaymentValue, clientID string) []*reduced.TotalPaymentValue {
	bufferID := "T3" + SEPERATOR + clientID
	referenceID := tpv.GetStoreId() + SEPERATOR + STORE
	_, allRefPresent := js.fullRefClients[clientID]
	if !allRefPresent {
		js.cacheService.BufferUnreferencedData(clientID, bufferID, tpv)
		return nil
	}
	protoRef, err := js.cacheService.GetRefData(clientID, referenceID)
	if err != nil {
		log.Errorf("Error retrieving reference data %s for client %s: %v", referenceID, clientID, err)
		return nil
	}
	joinedData := make([]*reduced.TotalPaymentValue, 0)
	store := utils.CastProtoMessage[*raw.Store](protoRef)
	tpv.StoreId = store.GetStoreName()
	joinedData = append(joinedData, tpv)
	// In case there are buffered profits waiting for this reference, resolve them now
	js.joinBufferedTPVData(clientID, bufferID, &joinedData)
	return joinedData
}

// This is T4
func (js *joinerService) JoinCountedUserTransactions(countedTransaction *reduced.CountedUserTransactions, clientID string) []*reduced.CountedUserTransactions {
	bufferID := "T4" + SEPERATOR + clientID
	storeRefID := countedTransaction.GetStoreId() + SEPERATOR + STORE
	userRefID := countedTransaction.GetUserId() + SEPERATOR + USER
	_, allRefPresent := js.fullRefClients[clientID]
	if !allRefPresent {
		js.cacheService.BufferUnreferencedData(clientID, bufferID, countedTransaction)
		return nil
	}
	storeProtoRef, err := js.cacheService.GetRefData(clientID, storeRefID)
	if err != nil {
		log.Errorf("Error retrieving reference data %s for client %s: %v", storeRefID, clientID, err)
		return nil
	}
	userProtoRef, err := js.cacheService.GetRefData(clientID, userRefID)
	if err != nil {
		log.Errorf("Error retrieving reference data %s for client %s: %v", userRefID, clientID, err)
		return nil
	}
	joinedData := make([]*reduced.CountedUserTransactions, 0)
	user := utils.CastProtoMessage[*raw.User](userProtoRef)
	store := utils.CastProtoMessage[*raw.Store](storeProtoRef)
	countedTransaction.Birthdate = user.GetBirthdate()
	countedTransaction.StoreId = store.GetStoreName()
	joinedData = append(joinedData, countedTransaction)
	// In case there are buffered profits waiting for this reference, resolve them now
	js.joinBufferedCountedTransactionsData(clientID, bufferID, &joinedData)
	return joinedData
}

/* --- Resource release --- */

func (js *joinerService) DeleteClientRefData(clientID string) error {
	js.cacheService.RemoveRefData(clientID)
	return nil
}

func (js *joinerService) Close() error {
	return js.cacheService.Close()
}

/* --- Buffered data Helper Functions --- */

func (js *joinerService) joinBufferedProfitData(clientID, bufferID string, joinedData *[]*reduced.TotalProfitBySubtotal) {
	js.cacheService.IterateUnreferencedData(clientID, bufferID, func(bufferedProto proto.Message) bool {
		bufferedProfit := utils.CastProtoMessage[*reduced.TotalProfitBySubtotal](bufferedProto)
		refID := bufferedProfit.GetItemId() + SEPERATOR + MENU_ITEM
		protoRef, err := js.cacheService.GetRefData(clientID, refID)
		if err != nil {
			log.Errorf("Error retrieving reference data %s for client %s: %v", refID, clientID, err)
			// TODO why return false, if this function is running it means the reference data must exist (done was already received)
			return false
		}
		menuItem := utils.CastProtoMessage[*raw.MenuItem](protoRef)
		bufferedProfit.ItemId = menuItem.GetItemName()
		*joinedData = append(*joinedData, bufferedProfit)
		return true // TODO no need to return true, as we want to remove all buffered data after reading it
	})
}

func (js *joinerService) joinBufferedSalesData(clientID, bufferID string, joinedData *[]*reduced.TotalSoldByQuantity) {
	js.cacheService.IterateUnreferencedData(clientID, bufferID, func(bufferedProto proto.Message) bool {
		bufferedSales := utils.CastProtoMessage[*reduced.TotalSoldByQuantity](bufferedProto)
		refID := bufferedSales.GetItemId() + SEPERATOR + MENU_ITEM
		protoRef, err := js.cacheService.GetRefData(clientID, refID)
		if err != nil {
			log.Errorf("Error retrieving reference data %s for client %s: %v", refID, clientID, err)
			// TODO why return false, if this function is running it means the reference data must exist (done was already received)
			return false
		}
		menuItem := utils.CastProtoMessage[*raw.MenuItem](protoRef)
		bufferedSales.ItemId = menuItem.GetItemName()
		*joinedData = append(*joinedData, bufferedSales)
		return true // TODO no need to return true, as we want to remove all buffered data after reading it
	})
}

func (js *joinerService) joinBufferedTPVData(clientID, bufferID string, joinedData *[]*reduced.TotalPaymentValue) {
	js.cacheService.IterateUnreferencedData(clientID, bufferID, func(bufferedProto proto.Message) bool {
		bufferedSales := utils.CastProtoMessage[*reduced.TotalPaymentValue](bufferedProto)
		refID := bufferedSales.GetStoreId() + SEPERATOR + STORE
		protoRef, err := js.cacheService.GetRefData(clientID, refID)
		if err != nil {
			log.Errorf("Error retrieving reference data %s for client %s: %v", refID, clientID, err)
			// TODO why return false, if this function is running it means the reference data must exist (done was already received)
			return false
		}
		store := utils.CastProtoMessage[*raw.Store](protoRef)
		bufferedSales.StoreId = store.GetStoreName()
		*joinedData = append(*joinedData, bufferedSales)
		return true // TODO no need to return true, as we want to remove all buffered data after reading it
	})
}

func (js *joinerService) joinBufferedCountedTransactionsData(clientID, bufferID string, joinedData *[]*reduced.CountedUserTransactions) {
	js.cacheService.IterateUnreferencedData(clientID, bufferID, func(bufferedProto proto.Message) bool {
		bufferedSales := utils.CastProtoMessage[*reduced.CountedUserTransactions](bufferedProto)
		storeRefID := bufferedSales.GetStoreId() + SEPERATOR + STORE
		userRefID := bufferedSales.GetUserId() + SEPERATOR + USER
		storeProtoRef, err := js.cacheService.GetRefData(clientID, storeRefID)
		if err != nil {
			log.Errorf("Error retrieving reference data %s for client %s: %v", storeRefID, clientID, err)
			// TODO why return false, if this function is running it means the reference data must exist (done was already received)
			return false
		}
		userProtoRef, err := js.cacheService.GetRefData(clientID, userRefID)
		if err != nil {
			log.Errorf("Error retrieving reference data %s for client %s: %v", userRefID, clientID, err)
			// TODO why return false, if this function is running it means the reference data must exist (done was already received)
			return false
		}
		user := utils.CastProtoMessage[*raw.User](userProtoRef)
		bufferedSales.Birthdate = user.GetBirthdate()
		store := utils.CastProtoMessage[*raw.Store](storeProtoRef)
		bufferedSales.StoreId = store.GetStoreName()
		*joinedData = append(*joinedData, bufferedSales)
		return true // TODO no need to return true, as we want to remove all buffered data after reading it
	})
}
