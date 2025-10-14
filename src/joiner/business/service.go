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
	cacheService cache.CacheService
}

func NewJoinerService(cacheService cache.CacheService) JoinerService {
	return &joinerService{
		cacheService: cacheService,
	}
}

/* --- Store raw reference data --- */

func storeRefData[T proto.Message](clientID string, items []T, getRefID func(T) string, cacheService cache.CacheService) error {
	for _, item := range items {
		protoItem := utils.ToProtoMessage(item)
		referenceID := getRefID(item)
		err := cacheService.StoreRefData(clientID, referenceID, protoItem)
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
	return nil
}

/* --- Get joined data --- */

// This is T2_1
func (js *joinerService) JoinTotalProfitBySubtotal(profit *reduced.TotalProfitBySubtotal, clientID string) []*reduced.TotalProfitBySubtotal {

	referenceID := profit.GetItemId() + SEPERATOR + MENU_ITEM
	protoRef, hasRef, err := js.cacheService.GetRefData(clientID, referenceID)

	if err != nil {
		log.Errorf("Error retrieving reference data %s for client %s: %v", referenceID, clientID, err)
		return nil
	}
	if !hasRef {
		log.Debugf("Reference for %s not found for client %s | Buffering until found", referenceID, clientID)
		clientID = "T2_1" + clientID
		js.cacheService.BufferUnreferencedData(clientID, referenceID, utils.ToProtoMessage(profit))
		return nil
	}

	joinedData := make([]*reduced.TotalProfitBySubtotal, 0)

	menuItem := utils.FromProtoMessage[*raw.MenuItem](protoRef)

	profit.ItemId = menuItem.GetItemName()
	joinedData = append(joinedData, profit)
	// In case there are buffered profits waiting for this reference,
	// we try to resolve them now
	clientID = "T2_1" + clientID
	js.cleanupUnreferencedProfit(clientID, referenceID, &joinedData)
	return joinedData
}

// This is T2_2
func (js *joinerService) JoinTotalSoldByQuantity(sales *reduced.TotalSoldByQuantity, clientID string) []*reduced.TotalSoldByQuantity {
	return nil
}

// This is T3
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

/* --- Cleanup Helper Functions --- */

func (js *joinerService) cleanupUnreferencedProfit(clientID, referenceID string, joinedData *[]*reduced.TotalProfitBySubtotal) {
	js.cacheService.IterateUnreferencedData(clientID, referenceID, func(bufferedProto *proto.Message) bool {
		bufferedProfit := utils.FromProtoMessage[*reduced.TotalProfitBySubtotal](bufferedProto)
		refID := bufferedProfit.GetItemId() + SEPERATOR + MENU_ITEM

		protoRef, hasRef, err := js.cacheService.GetRefData(clientID, refID)
		if err != nil {
			log.Errorf("Error retrieving reference data %s for client %s: %v", refID, clientID, err)
			return false
		}
		if !hasRef {
			return false
		}
		menuItem := utils.FromProtoMessage[*raw.MenuItem](protoRef)
		bufferedProfit.ItemId = menuItem.GetItemName()
		*joinedData = append(*joinedData, bufferedProfit)
		return true
	})
}
