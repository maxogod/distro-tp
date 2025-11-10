package cache

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"google.golang.org/protobuf/proto"
)

// storage holds the data for each client.
// It contains a map of reference IDs to their corresponding data.
type storage struct {
	userReferenceData      map[string]*raw.User
	menuItemsReferenceData map[string]*raw.MenuItem
	storeReferenceData     map[string]*raw.Store
}

// inMemoryCache is a CacheService implementation provides fast in-memory storage of data.
type inMemoryCache struct {
	clientStorage map[string]storage
}

func NewInMemoryCache() InMemoryService {
	return &inMemoryCache{
		clientStorage: make(map[string]storage),
	}
}

func (in *inMemoryCache) getClientStorage(clientID string) *storage {
	if _, exists := in.clientStorage[clientID]; !exists {
		in.clientStorage[clientID] = storage{
			userReferenceData:      make(map[string]*raw.User),
			menuItemsReferenceData: make(map[string]*raw.MenuItem),
			storeReferenceData:     make(map[string]*raw.Store),
		}
	}
	s := in.clientStorage[clientID]
	return &s
}

func storeData[T proto.Message](mapData map[string]T, data []T, getRefKey func(T) string) {
	for _, item := range data {
		refKey := getRefKey(item)
		mapData[refKey] = item
	}
}

func (in *inMemoryCache) StoreMenuItems(clientID string, data []*raw.MenuItem) {
	clientStorage := in.getClientStorage(clientID)
	getRefKey := func(item *raw.MenuItem) string {
		return item.GetItemId()
	}
	storeData(clientStorage.menuItemsReferenceData, data, getRefKey)
}

func (in *inMemoryCache) StoreShops(clientID string, data []*raw.Store) {
	clientStorage := in.getClientStorage(clientID)
	getRefKey := func(item *raw.Store) string {
		return item.GetStoreId()
	}
	storeData(clientStorage.storeReferenceData, data, getRefKey)
}

func (in *inMemoryCache) StoreUsers(clientID string, data []*raw.User) {
	clientStorage := in.getClientStorage(clientID)
	getRefKey := func(item *raw.User) string {
		return item.GetUserId()
	}
	storeData(clientStorage.userReferenceData, data, getRefKey)
}

func (in *inMemoryCache) GetMenuItem(clientID string) (map[string]*raw.MenuItem, error) {
	clientStorage := in.getClientStorage(clientID)
	if len(clientStorage.menuItemsReferenceData) == 0 {
		return nil, fmt.Errorf("no menu item reference data found for client %s", clientID)
	}
	return clientStorage.menuItemsReferenceData, nil

}

func (in *inMemoryCache) GetShop(clientID string) (map[string]*raw.Store, error) {
	clientStorage := in.getClientStorage(clientID)

	if len(clientStorage.storeReferenceData) == 0 {
		return nil, fmt.Errorf("no store reference data found for client %s", clientID)
	}

	return clientStorage.storeReferenceData, nil
}

func (in *inMemoryCache) GetUser(clientID string) (map[string]*raw.User, error) {
	clientStorage := in.getClientStorage(clientID)
	if len(clientStorage.userReferenceData) == 0 {
		return nil, fmt.Errorf("no user reference data found for client %s", clientID)
	}
	return clientStorage.userReferenceData, nil
}

func (in *inMemoryCache) RemoveRefData(clientID string, referenceType enum.ReferenceType) {
	clientStorage := in.getClientStorage(clientID)
	switch referenceType {
	case enum.MenuItems:
		for k := range clientStorage.menuItemsReferenceData {
			delete(clientStorage.menuItemsReferenceData, k)
		}
	case enum.Users:
		for k := range clientStorage.userReferenceData {
			delete(clientStorage.userReferenceData, k)
		}
	case enum.Stores:
		for k := range clientStorage.storeReferenceData {
			delete(clientStorage.storeReferenceData, k)
		}
	}
}

func (in *inMemoryCache) Close() error {
	for clientID := range in.clientStorage {
		delete(in.clientStorage, clientID)
	}
	return nil
}
