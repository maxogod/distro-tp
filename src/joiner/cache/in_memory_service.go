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
	clientStorage map[string]*storage
}

func NewInMemoryCache() InMemoryService {
	return &inMemoryCache{
		clientStorage: make(map[string]*storage),
	}
}

func (in *inMemoryCache) getClientStorage(clientID string) *storage {
	if _, exists := in.clientStorage[clientID]; !exists {
		in.clientStorage[clientID] = &storage{
			userReferenceData:      make(map[string]*raw.User),
			menuItemsReferenceData: make(map[string]*raw.MenuItem),
			storeReferenceData:     make(map[string]*raw.Store),
		}
	}
	return in.clientStorage[clientID] // return pointer to the real struct
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

func (in *inMemoryCache) GetMenuItem(clientID string, itemID string) (*raw.MenuItem, error) {
	clientStorage := in.getClientStorage(clientID)
	menuItem, exists := clientStorage.menuItemsReferenceData[itemID]
	if !exists {
		return nil, fmt.Errorf("menu item %s not found for client %s", itemID, clientID)
	}
	return menuItem, nil

}

func (in *inMemoryCache) GetShop(clientID string, shopID string) (*raw.Store, error) {
	clientStorage := in.getClientStorage(clientID)
	store, exists := clientStorage.storeReferenceData[shopID]
	if !exists {
		return nil, fmt.Errorf("store %s not found for client %s", shopID, clientID)
	}
	return store, nil
}

func (in *inMemoryCache) GetUser(clientID string, userID string) (*raw.User, error) {
	clientStorage := in.getClientStorage(clientID)
	user, exists := clientStorage.userReferenceData[userID]
	if !exists {
		return nil, fmt.Errorf("user %s not found for client %s", userID, clientID)
	}
	return user, nil
}

func (in *inMemoryCache) RemoveRefData(clientID string, referenceType enum.ReferenceType) {
	clientStorage := in.getClientStorage(clientID)
	switch referenceType {
	case enum.MenuItems:
		clientStorage.menuItemsReferenceData = make(map[string]*raw.MenuItem)
	case enum.Users:
		clientStorage.userReferenceData = make(map[string]*raw.User)
	case enum.Stores:
		clientStorage.storeReferenceData = make(map[string]*raw.Store)
	}
	in.clientStorage[clientID] = clientStorage
}

func (in *inMemoryCache) RemoveAllRefData(clientID string) {
	delete(in.clientStorage, clientID)
}

func (in *inMemoryCache) Close() error {
	for clientID := range in.clientStorage {
		delete(in.clientStorage, clientID)
	}
	return nil
}
