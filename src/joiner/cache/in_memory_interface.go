package cache

import (
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

// The InMemoryService interface defines the methods that any cache implementation must provide.
// This allows the business logic to interact with different caching mechanisms
// without being tightly coupled to a specific implementation.
// InMemoryService defines the contract for an in-memory caching service that stores
// and retrieves reference data for clients. It provides operations to manage menu items,
// shops, and users, with the ability to remove specific reference types and close resources.
type InMemoryService interface {
	// StoreMenuItems stores a collection of menu items for a specific client.
	// It takes the client ID and a slice of menu items to persist in the cache.
	// Returns an error if the storage operation fails.
	StoreMenuItems(clientID string, data []*raw.MenuItem) error

	// StoreShops stores a collection of shops for a specific client.
	// It takes the client ID and a slice of shops to persist in the cache.
	// Returns an error if the storage operation fails.
	StoreShops(clientID string, data []*raw.Store) error

	// StoreUsers stores a collection of users for a specific client.
	// It takes the client ID and a slice of users to persist in the cache.
	// Returns an error if the storage operation fails.
	StoreUsers(clientID string, data []*raw.User) error

	// GetMenuItem retrieves all menu items stored for a specific client.
	// Returns a map of menu items indexed by their identifier and an error if the retrieval fails.
	GetMenuItem(clientID string) (map[string]*raw.MenuItem, error)

	// GetShop retrieves all shops stored for a specific client.
	// Returns a map of shops indexed by their identifier and an error if the retrieval fails.
	GetShop(clientID string) (map[string]*raw.Store, error)

	// GetUser retrieves all users stored for a specific client.
	// Returns a map of users indexed by their identifier and an error if the retrieval fails.
	GetUser(clientID string) (map[string]*raw.User, error)

	// RemoveRefData removes all reference data of a specific type for a given client.
	// The referenceType parameter specifies which type of data to remove (menu items, shops, or users).
	RemoveRefData(clientID string, referenceType enum.ReferenceType)

	// Close releases all resources held by the in-memory service.
	// Returns an error if the cleanup operation fails.
	Close() error
}
