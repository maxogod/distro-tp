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
	referenceData map[enum.ReferenceType]map[string]proto.Message
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

func storeData[T proto.Message](in *inMemoryCache, clientID string, referenceType enum.ReferenceType, data []T, getRefKey func(T) string) error {
	for _, item := range data {
		// Initialize storage for the client if it doesn't exist
		if _, exists := in.clientStorage[clientID]; !exists {
			in.clientStorage[clientID] = storage{
				referenceData: make(map[enum.ReferenceType]map[string]proto.Message),
			}
		}
		// Initialize storage for the reference type if it doesn't exist
		if _, exists := in.clientStorage[clientID].referenceData[referenceType]; !exists {
			in.clientStorage[clientID].referenceData[referenceType] = make(map[string]proto.Message)
		}
		// Store the item with the appropriate key
		in.clientStorage[clientID].referenceData[referenceType][getRefKey(item)] = item
	}
	return nil
}

func (in *inMemoryCache) StoreMenuItems(clientID string, data []*raw.MenuItem) error {
	getRefKey := func(item *raw.MenuItem) string {
		return item.GetItemId()
	}
	return storeData(in, clientID, enum.MenuItems, data, getRefKey)
}

func (in *inMemoryCache) StoreShops(clientID string, data []*raw.Store) error {
	getRefKey := func(item *raw.Store) string {
		return item.GetStoreId()
	}
	return storeData(in, clientID, enum.Stores, data, getRefKey)
}

func (in *inMemoryCache) StoreUsers(clientID string, data []*raw.User) error {
	getRefKey := func(item *raw.User) string {
		return item.GetUserId()
	}
	return storeData(in, clientID, enum.Users, data, getRefKey)
}

func (in *inMemoryCache) GetMenuItem(clientID string) (map[string]*raw.MenuItem, error) {
	referenceData, ok := in.clientStorage[clientID].referenceData[enum.MenuItems]
	if !ok {
		return nil, fmt.Errorf("no menu items found for client %s", clientID)
	}
	result := make(map[string]*raw.MenuItem)
	for key, value := range referenceData {
		result[key] = value.(*raw.MenuItem)
	}
	return result, nil
}

func (in *inMemoryCache) GetShop(clientID string) (map[string]*raw.Store, error) {
	referenceData, ok := in.clientStorage[clientID].referenceData[enum.Stores]
	if !ok {
		return nil, fmt.Errorf("no shops found for client %s", clientID)
	}
	result := make(map[string]*raw.Store)
	for key, value := range referenceData {
		result[key] = value.(*raw.Store)
	}
	return result, nil
}

func (in *inMemoryCache) GetUser(clientID string) (map[string]*raw.User, error) {
	referenceData, ok := in.clientStorage[clientID].referenceData[enum.Users]
	if !ok {
		return nil, fmt.Errorf("no users found for client %s", clientID)
	}
	result := make(map[string]*raw.User)
	for key, value := range referenceData {
		result[key] = value.(*raw.User)
	}
	return result, nil
}

func (in *inMemoryCache) RemoveRefData(clientID string, referenceType enum.ReferenceType) {
	if clientStorage, exists := in.clientStorage[clientID]; exists {
		delete(clientStorage.referenceData, referenceType)
	}

}

func (in *inMemoryCache) Close() error {
	for clientID := range in.clientStorage {
		delete(in.clientStorage, clientID)
	}
	return nil
}
