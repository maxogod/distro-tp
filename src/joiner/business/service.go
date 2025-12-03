package business

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/worker/storage"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"google.golang.org/protobuf/proto"
)

const (
	MENU_ITEMS_REF = "@MenuItems"
	STORES_REF     = "@Stores"
	USERS_REF      = "@Users"

	STORAGE_FOLDER_PATH        = "storage/"
	IN_PROGRESS_FILE_EXTENSION = ".inprogress"
)

type joinerService struct {
	inMemoryService      cache.InMemoryService
	storageService       storage.StorageService
	clientRefs           map[string]map[string]bool
	fullRefClients       map[string]bool
	userStorageGroupSize int
}

func NewJoinerService(inMemoryService cache.InMemoryService, storageService storage.StorageService, userStorageGroupSize int) JoinerService {
	js := &joinerService{
		inMemoryService:      inMemoryService,
		storageService:       storageService,
		fullRefClients:       make(map[string]bool),
		clientRefs:           make(map[string]map[string]bool),
		userStorageGroupSize: userStorageGroupSize,
	}

	// Setup fullRefClients based on existing storage files
	allClientsRefs := storageService.GetAllFilesReferences()
	logger.Logger.Infof("Preexisting clients references found on disk: %d", len(allClientsRefs))
	for _, ref := range allClientsRefs {
		clientID := strings.Split(ref, "@")[0]
		if clientDone(clientID) {
			js.fullRefClients[clientID] = true
			logger.Logger.Debugf("Preexisting Client %s marked as having full reference data on startup", clientID)
		} else {
			logger.Logger.Debugf("Preexisting Client %s does NOT have full reference data on startup", clientID)
		}
	}

	return js
}

// ======= GENERIC HELPERS (Private) =======

// Usage in joinerService methods:
func (js *joinerService) StoreMenuItems(clientID string, items []*raw.MenuItem) error {
	referenceID := clientID + MENU_ITEMS_REF
	js.trackClientRef(clientID, referenceID)
	return storage.StoreBatch(js.storageService, referenceID, items)
}

func (js *joinerService) StoreShops(clientID string, items []*raw.Store) error {
	referenceID := clientID + STORES_REF
	js.trackClientRef(clientID, referenceID)
	return storage.StoreBatch(js.storageService, referenceID, items)
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
		err := storage.StoreBatch(js.storageService, referenceID, groupUsers)
		if err != nil {
			return fmt.Errorf("error storing users for group %d: %w", groupNum, err)
		}
		js.trackClientRef(clientID, referenceID)
	}
	return nil
}

func (js *joinerService) FinishStoringRefData(clientID string) error {
	logger.Logger.Debug("Received all reference data for client: ", clientID)
	js.fullRefClients[clientID] = true // All reference data was received for this client
	createOrRemoveProgressFile(clientID, false)
	return nil
}

func (js *joinerService) SyncData() error {
	logger.Logger.Debug("Syncing all reference data to disk . . .")
	for clientID := range js.clientRefs {
		for ref := range js.clientRefs[clientID] {
			if err := js.storageService.FlushWriting(ref); err != nil {
				return err
			}
		}
	}
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
	groupNum := ((int(userNum)-1)/js.userStorageGroupSize + 1) * js.userStorageGroupSize
	return groupNum, nil
}

func (js *joinerService) trackClientRef(clientID string, ref string) {
	if _, exists := js.clientRefs[clientID]; !exists {
		js.clientRefs[clientID] = make(map[string]bool)
		createOrRemoveProgressFile(clientID, true)
	}
	js.clientRefs[clientID][ref] = true
}

// ======== Get reference data from disk =========

func loadReferenceData[T proto.Message](js *joinerService, referenceID string, factory func() T, getKey func(T) string) (map[string]T, []T, error) {

	read_ch, err := js.storageService.ReadAllData(referenceID)
	if err != nil {
		return nil, nil, err
	}
	result := make(map[string]T)
	resultsList := make([]T, 0)

	for protoBytes := range read_ch {
		protoData := factory()
		err := proto.Unmarshal(protoBytes, protoData)
		if err != nil {
			logger.Logger.Warnf("Error unmarshalling proto message: %v", err)
			continue
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
		actualRef, err := js.inMemoryService.GetMenuItem(clientID, menuItemId)
		if err != nil {
			return nil, fmt.Errorf("error retrieving menu %s from in-memory cache after loading from disk: %w", menuItemId, err)
		}

		return actualRef, nil

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

		actualRef, err := js.inMemoryService.GetShop(clientID, shopId)
		if err != nil {
			return nil, fmt.Errorf("error retrieving shop %s from in-memory cache after loading from disk: %w", shopId, err)
		}

		return actualRef, nil
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
		//logger.Logger.Debugf("DATA MISS for userID: %s Loading users for client %s from disk, group %d", userId, clientID, groupNum)
		diskUsers, usersList, err := loadReferenceData(js, referenceID, factory, getRefKey)
		if err != nil {
			logger.Logger.Debugf("Error loading users for client %s: %v", clientID, err)
			return nil, err
		}
		// We first remove all data from the current cache, then store a new batch of users to the cache
		js.inMemoryService.RemoveRefData(clientID, enum.Users)
		js.inMemoryService.StoreUsers(clientID, usersList)
		logger.Logger.Debugf("Amount of users loaded for client %s: %d", clientID, len(diskUsers))

		actualRef, err := js.inMemoryService.GetUser(clientID, userId)
		if err != nil {
			return nil, fmt.Errorf("error retrieving user %s from in-memory cache after loading from disk: %w", userId, err)
		}

		return actualRef, nil
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
	js.inMemoryService.RemoveAllRefData(clientID)
	js.storageService.RemoveCache(clientID)
	for ref := range js.clientRefs[clientID] {
		js.storageService.StopWriting(ref)
	}
	return nil
}

func (js *joinerService) Close() error {
	js.storageService.Close()
	return js.inMemoryService.Close()
}

/* --- HELPERS --- */

func createOrRemoveProgressFile(clientID string, create bool) error {
	filePath := STORAGE_FOLDER_PATH + clientID + IN_PROGRESS_FILE_EXTENSION
	if create {
		file, err := os.Create(filePath)
		if err != nil {
			return err
		}
		defer file.Close()
	} else {
		err := os.Remove(filePath)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func clientDone(clientID string) bool {
	_, err := os.Stat(STORAGE_FOLDER_PATH + clientID + IN_PROGRESS_FILE_EXTENSION)
	return err != nil // If progress file exists, then its not done
}
