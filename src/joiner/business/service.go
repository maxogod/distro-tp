package business

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"google.golang.org/protobuf/proto"
)

const SEPERATOR = "@"

type joinerService struct {
	cacheService   cache.CacheService
	fullRefClients map[string]bool // Used as a set
	datasets       config.ReferenceDatasets
}

func NewJoinerService(cacheService cache.CacheService, datasets config.ReferenceDatasets) JoinerService {
	return &joinerService{
		cacheService:   cacheService,
		fullRefClients: make(map[string]bool),
		datasets:       datasets,
	}
}

/* --- Store raw reference data --- */

func storeRefData[T proto.Message](clientID string, items []T, getRefID func(T) string, cacheService cache.CacheService) error {
	for _, item := range items {
		referenceID := getRefID(item)
		err := cacheService.StoreRefData(clientID, referenceID, item)
		if err != nil {
			logger.Logger.Debugf("Failed to store ref data %s for client %s: %v", referenceID, clientID, err)
			return err
		}
	}
	return nil
}

// Usage in joinerService methods:
func (js *joinerService) StoreMenuItems(clientID string, items []*raw.MenuItem) error {
	return storeRefData(clientID, items, func(item *raw.MenuItem) string {
		return item.ItemId + SEPERATOR + js.datasets.MenuItem
	}, js.cacheService)
}

func (js *joinerService) StoreShops(clientID string, items []*raw.Store) error {
	return storeRefData(clientID, items, func(item *raw.Store) string {
		return item.StoreId + SEPERATOR + js.datasets.Store
	}, js.cacheService)
}

func (js *joinerService) StoreUsers(clientID string, items []*raw.User) error {
	return storeRefData(clientID, items, func(item *raw.User) string {
		return item.UserId + SEPERATOR + js.datasets.User
	}, js.cacheService)
}

func (js *joinerService) FinishStoringRefData(clientID string) error {
	logger.Logger.Debug("Received all reference data for client: ", clientID)
	js.fullRefClients[clientID] = true // All reference data was received for this client
	return nil
}

/* --- Get joined data --- */

// This is T2
func (js *joinerService) JoinTotalSumItem(sales *reduced.TotalSumItem, clientID string) (*reduced.TotalSumItem, error) {
	bufferID := "T2_2" + SEPERATOR + clientID
	referenceID := sales.GetItemId() + SEPERATOR + js.datasets.MenuItem
	_, allRefPresent := js.fullRefClients[clientID]
	if !allRefPresent {
		js.cacheService.BufferUnreferencedData(clientID, bufferID, sales)
		return nil, fmt.Errorf("not all reference data present for client %s", clientID)
	}
	protoRef, err := js.cacheService.GetRefData(clientID, referenceID)
	if err != nil {
		logger.Logger.Debugf("Error retrieving reference data %s for client %s: %v", referenceID, clientID, err)
		return nil, err
	}
	menuItem := utils.CastProtoMessage[*raw.MenuItem](protoRef)
	sales.ItemId = menuItem.GetItemName()
	return sales, nil
}

// This is T3
func (js *joinerService) JoinTotalPaymentValue(tpv *reduced.TotalPaymentValue, clientID string) (*reduced.TotalPaymentValue, error) {
	referenceID := tpv.GetStoreId() + SEPERATOR + js.datasets.Store
	_, allRefPresent := js.fullRefClients[clientID]
	if !allRefPresent {
		return nil, fmt.Errorf("not all reference data present for client %s", clientID)
	}
	protoRef, err := js.cacheService.GetRefData(clientID, referenceID)
	if err != nil {
		logger.Logger.Debugf("Error retrieving reference data %s for client %s: %v", referenceID, clientID, err)
		return nil, err
	}
	store := utils.CastProtoMessage[*raw.Store](protoRef)
	tpv.StoreId = store.GetStoreName()
	return tpv, nil
}

// This is T4
func (js *joinerService) JoinCountedUserTransactions(countedTransaction *reduced.CountedUserTransactions, clientID string) (*reduced.CountedUserTransactions, error) {
	storeRefID := countedTransaction.GetStoreId() + SEPERATOR + js.datasets.Store
	userRefID := countedTransaction.GetUserId() + SEPERATOR + js.datasets.User
	_, allRefPresent := js.fullRefClients[clientID]
	if !allRefPresent {
		return nil, fmt.Errorf("not all reference data present for client %s", clientID)
	}
	storeProtoRef, err := js.cacheService.GetRefData(clientID, storeRefID)
	if err != nil {
		logger.Logger.Debugf("Error retrieving reference data %s for client %s: %v", storeRefID, clientID, err)
		return nil, err
	}
	userProtoRef, err := js.cacheService.GetRefData(clientID, userRefID)
	if err != nil {
		logger.Logger.Debugf("Error retrieving reference data %s for client %s: %v", userRefID, clientID, err)
		return nil, err
	}
	user := utils.CastProtoMessage[*raw.User](userProtoRef)
	store := utils.CastProtoMessage[*raw.Store](storeProtoRef)
	countedTransaction.Birthdate = user.GetBirthdate()
	countedTransaction.StoreId = store.GetStoreName()
	return countedTransaction, nil
}

/* --- Resource release --- */

func (js *joinerService) DeleteClientRefData(clientID string) error {
	js.cacheService.RemoveRefData(clientID)
	return nil
}

func (js *joinerService) Close() error {
	return js.cacheService.Close()
}
