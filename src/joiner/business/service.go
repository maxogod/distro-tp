package business

import (
	"strconv"

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
		return strconv.Itoa(int(item.UserId)) + SEPERATOR + USER
	}, js.cacheService)
}

func (js *joinerService) FinishStoringRefData(clientID string) error {
	js.fullRefClients[clientID] = true // All reference data was received for this client
	return nil
}

/* --- Get joined data --- */

func (js *joinerService) JoinTotalProfitBySubtotal(profit *reduced.TotalProfitBySubtotal, clientID string) []*reduced.TotalProfitBySubtotal {
	referenceID := profit.GetItemId() + SEPERATOR + MENU_ITEM
	_, exists := js.fullRefClients[clientID]
	if !exists {
		js.cacheService.BufferUnreferencedData(clientID, referenceID, profit)
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
	// TODO this is used to get from cache with that ID but we never inserted data with that ID (it wont exists in map)
	clientID = "T2_1" + clientID
	js.joinBufferedProfitData(clientID, referenceID, &joinedData)
	return joinedData
}

func (js *joinerService) JoinTotalSoldByQuantity(sales *reduced.TotalSoldByQuantity, clientID string) []*reduced.TotalSoldByQuantity {
	return nil
}

func (js *joinerService) JoinTotalPaymentValue(tpv *reduced.TotalPaymentValue, clientID string) []*reduced.TotalPaymentValue {
	return nil
}

func (js *joinerService) JoinCountedUserTransactions(countedTransaction *reduced.CountedUserTransactions, clientID string) []*reduced.CountedUserTransactions {
	return nil
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

func (js *joinerService) joinBufferedProfitData(clientID, referenceID string, joinedData *[]*reduced.TotalProfitBySubtotal) {
	// TODO why is referenceID ignored
	js.cacheService.IterateUnreferencedData(clientID, referenceID, func(bufferedProto proto.Message) bool {
		// TODO why is this referenceID ignored as well

		// TODO how can we assure this type is the correct one?
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
