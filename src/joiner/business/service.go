package business

import (
	"fmt"
	"strconv"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	storage "github.com/maxogod/distro-tp/src/common/worker/storage"
	store_helper "github.com/maxogod/distro-tp/src/common/worker/storage/disk_memory"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"google.golang.org/protobuf/proto"
)

const MENU_ITEMS_REF = "MenuItems"
const STORES_REF = "Stores"
const USERS_REF = "Users"

type joinerService struct {
	inMemoryService      cache.InMemoryService
	storageService       storage.StorageService
	fullRefClients       map[string]bool // Used as a set
	userStorageGroupSize int
}

func NewJoinerService(inMemoryService cache.InMemoryService, storageService storage.StorageService, userStorageGroupSize int) JoinerService {
	return &joinerService{
		inMemoryService:      inMemoryService,
		storageService:       storageService,
		fullRefClients:       make(map[string]bool),
		userStorageGroupSize: userStorageGroupSize,
	}
}

// ======= GENERIC HELPERS (Private) =======

// Usage in joinerService methods:
func (js *joinerService) StoreMenuItems(clientID string, items []*raw.MenuItem) error {
	referenceID := clientID + MENU_ITEMS_REF
	return store_helper.StoreBatch(js.storageService, referenceID, items)
}

func (js *joinerService) StoreShops(clientID string, items []*raw.Store) error {
	referenceID := clientID + STORES_REF
	return store_helper.StoreBatch(js.storageService, referenceID, items)
}

func (js *joinerService) StoreUsers(clientID string, items []*raw.User) error {

	// since the users dataset is too big to load in memory, we store them in multiple files
	// each file will contain users from a specific group determined by their user ID
	// the amount of users per group is defined by user_storage_groups config parameter
	userGroups := make(map[int][]*raw.User)
	for _, user := range items {
		groupNum, err := js.getUsersGroup(user.GetUserId())
		if err != nil {
			return fmt.Errorf("error getting user group for user %s: %w", user.GetUserId(), err)
		}
		userGroups[groupNum] = append(userGroups[groupNum], user)
	}
	for groupNum, groupUsers := range userGroups {
		referenceID := fmt.Sprintf("%s%s%d", clientID, USERS_REF, groupNum)
		err := store_helper.StoreBatch(js.storageService, referenceID, groupUsers)
		if err != nil {
			return fmt.Errorf("error storing users for group %d: %w", groupNum, err)
		}
	}
	return nil
}

func (js *joinerService) FinishStoringRefData(clientID string) error {
	logger.Logger.Debug("Received all reference data for client: ", clientID)
	js.fullRefClients[clientID] = true // All reference data was received for this client
	return nil
}

// ======= PRIVATE METHODS =======

func (js *joinerService) hasAllReferenceData(clientID string) bool {
	_, exists := js.fullRefClients[clientID]
	return exists
}

func (js *joinerService) getUsersGroup(userID string) (int, error) {
	userNum, err := strconv.ParseFloat(userID, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse userID: %w", err)
	}
	groupNum := ((int(userNum) / js.userStorageGroupSize) + 1) * js.userStorageGroupSize
	return groupNum, nil
}

// ======== Get reference data from disk =========

func loadReferenceData[T proto.Message](js *joinerService, referenceID string, factory func() T, getKey func(T) string) (map[string]T, []T, error) {

	read_ch := make(chan []byte)
	js.storageService.ReadData(referenceID, read_ch)
	result := make(map[string]T)
	resultsList := make([]T, 0)

	for protoBytes := range read_ch {
		protoData := factory()
		err := proto.Unmarshal(protoBytes, protoData)
		if err != nil {
			logger.Logger.Errorf("Error unmarshalling proto message: %v", err)
			return nil, nil, err
		}
		result[getKey(protoData)] = protoData
		resultsList = append(resultsList, protoData)
	}
	return result, resultsList, nil
}

// ======== Get reference data ========

func (js *joinerService) getMenuItemRef(clientID string, menuItemId string) (*raw.MenuItem, error) {
	inMemoryMenuItemRef, err := js.inMemoryService.GetMenuItem(clientID, menuItemId)
	if err != nil { // no data in in-memory, so now we load from persistent storage
		logger.Logger.Debugf("DATA MISS for menuItemID: %s Loading menu items for client %s from disk", menuItemId, clientID)
		factory := func() *raw.MenuItem {
			return &raw.MenuItem{}
		}
		getRefKey := func(item *raw.MenuItem) string {
			return item.GetItemId()
		}
		referenceID := clientID + MENU_ITEMS_REF
		diskMenuItems, menuItemsList, err := loadReferenceData(js, referenceID, factory, getRefKey)
		if err != nil {
			logger.Logger.Debugf("Error loading menu items for client %s: %v", clientID, err)
			return nil, err
		}
		js.inMemoryService.StoreMenuItems(clientID, menuItemsList)
		logger.Logger.Debugf("Amount of menu items loaded for client %s: %d", clientID, len(diskMenuItems))
		return diskMenuItems[menuItemId], nil
	}
	return inMemoryMenuItemRef, nil
}

func (js *joinerService) getShopRef(clientID string, shopId string) (*raw.Store, error) {
	inMemoryStoreRef, err := js.inMemoryService.GetShop(clientID, shopId)
	if err != nil { // no data in in-memory, so now we load from persistent storage
		logger.Logger.Debugf("DATA MISS for storeID: %s Loading stores for client %s from disk", shopId, clientID)
		factory := func() *raw.Store {
			return &raw.Store{}
		}
		getRefKey := func(item *raw.Store) string {
			return item.GetStoreId()
		}
		referenceID := clientID + STORES_REF
		diskStores, storesList, err := loadReferenceData(js, referenceID, factory, getRefKey)
		if err != nil {
			logger.Logger.Debugf("Error loading menu items for client %s: %v", clientID, err)
			return nil, err
		}
		js.inMemoryService.StoreShops(clientID, storesList)
		logger.Logger.Debugf("Amount of stores loaded for client %s: %d", clientID, len(diskStores))
		return diskStores[shopId], nil
	}
	return inMemoryStoreRef, nil
}

func (js *joinerService) getUserRef(clientID string, userId string) (*raw.User, error) {

	inMemoryUserRef, err := js.inMemoryService.GetUser(clientID, userId)
	if err != nil { // no data in in-memory, so now we load from persistent storage
		factory := func() *raw.User {
			return &raw.User{}
		}
		getRefKey := func(item *raw.User) string {
			return item.GetUserId()
		}
		groupNum, err := js.getUsersGroup(userId)
		if err != nil {
			return nil, fmt.Errorf("error getting user group for user %s: %w", userId, err)
		}
		referenceID := fmt.Sprintf("%s%s%d", clientID, USERS_REF, groupNum)
		logger.Logger.Debugf("DATA MISS for userID: %s Loading users for client %s from disk, group %d", userId, clientID, groupNum)
		diskUsers, usersList, err := loadReferenceData(js, referenceID, factory, getRefKey)
		if err != nil {
			logger.Logger.Debugf("Error loading users for client %s: %v", clientID, err)
			return nil, err
		}
		// We first remove all data from the current cache, then store a new batch of users to the cache
		js.inMemoryService.RemoveRefData(clientID, enum.Users)
		js.inMemoryService.StoreUsers(clientID, usersList)
		logger.Logger.Debugf("Amount of users loaded for client %s: %d", clientID, len(diskUsers))
		return diskUsers[userId], nil
	}
	return inMemoryUserRef, nil
}

// ======= JOINING FUNCTIONS =======

// This is T2
func (js *joinerService) JoinTotalSumItem(sales *reduced.TotalSumItem, clientID string) error {

	if !js.hasAllReferenceData(clientID) {
		return fmt.Errorf("not all reference data present for client %s", clientID)
	}

	menuItem, err := js.getMenuItemRef(clientID, sales.GetItemId())
	if err != nil {
		logger.Logger.Debugf("Error retrieving reference data %s for client %s: %v", sales.GetItemId(), clientID, err)
		return err
	}
	sales.ItemId = menuItem.GetItemName()
	return nil
}

// This is T3
func (js *joinerService) JoinTotalPaymentValue(tpv *reduced.TotalPaymentValue, clientID string) error {
	if !js.hasAllReferenceData(clientID) {
		return fmt.Errorf("not all reference data present for client %s", clientID)
	}
	store, err := js.getShopRef(clientID, tpv.GetStoreId())
	if err != nil {
		logger.Logger.Debugf("Error retrieving reference data %s for client %s: %v", tpv.GetStoreId(), clientID, err)
		return err
	}
	tpv.StoreId = store.GetStoreName()
	return nil
}

// This is T4
func (js *joinerService) JoinCountedUserTransactions(countedTransaction *reduced.CountedUserTransactions, clientID string) error {

	if !js.hasAllReferenceData(clientID) {
		return fmt.Errorf("not all reference data present for client %s", clientID)
	}
	user, err := js.getUserRef(clientID, countedTransaction.GetUserId())
	if err != nil {
		logger.Logger.Debugf("Error retrieving reference data %s for client %s: %v", countedTransaction.GetUserId(), clientID, err)
		return err
	}
	store, err := js.getShopRef(clientID, countedTransaction.GetStoreId())
	if err != nil {
		logger.Logger.Debugf("Error retrieving reference data %s for client %s: %v", countedTransaction.GetStoreId(), clientID, err)
		return err
	}
	countedTransaction.Birthdate = user.GetBirthdate()
	countedTransaction.StoreId = store.GetStoreName()
	return nil
}

/* --- Resource release --- */

func (js *joinerService) DeleteClientRefData(clientID string) error {
	js.storageService.RemoveCache(clientID)
	return nil
}

func (js *joinerService) Close() error {
	js.storageService.Close()
	return js.inMemoryService.Close()
}
