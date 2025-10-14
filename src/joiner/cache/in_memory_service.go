package cache

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// storage holds the data for each client.
// It contains a map of reference IDs to their corresponding data.
type storage struct {
	referenceData    map[string]proto.Message   // key is the ID or reference
	unreferencedData map[string][]proto.Message // data without a reference
}

// inMemoryCache is a CacheService implementation provides fast in-memory storage of data.
type inMemoryCache struct {
	memoryStorage map[string]storage
}

func NewInMemoryCache() CacheService {
	return &inMemoryCache{
		memoryStorage: make(map[string]storage),
	}
}

func (c *inMemoryCache) StoreRefData(clientID string, referenceID string, data proto.Message) error {
	if _, exists := c.memoryStorage[clientID]; !exists {
		c.memoryStorage[clientID] = storage{
			referenceData:    make(map[string]proto.Message),
			unreferencedData: make(map[string][]proto.Message),
		}
	}
	clientStorage := c.memoryStorage[clientID]
	clientStorage.referenceData[referenceID] = data
	c.memoryStorage[clientID] = clientStorage

	return nil
}

func (c *inMemoryCache) GetRefData(clientID string, referenceID string) (proto.Message, error) {
	clientStorage, clientExists := c.memoryStorage[clientID]
	if !clientExists {
		return nil, fmt.Errorf("clientID '%s' does not exist", clientID)
	}
	data, refExists := clientStorage.referenceData[referenceID]
	if !refExists {
		return nil, nil
	}
	return data, nil
}

func (c *inMemoryCache) RemoveRefData(clientID string) {
	delete(c.memoryStorage, clientID)
}

func (c *inMemoryCache) BufferUnreferencedData(clientID string, referenceID string, data proto.Message) error {

	if _, exists := c.memoryStorage[clientID]; !exists {
		c.memoryStorage[clientID] = storage{
			referenceData:    make(map[string]proto.Message),
			unreferencedData: make(map[string][]proto.Message),
		}
	}
	clientStorage := c.memoryStorage[clientID]
	clientStorage.unreferencedData[referenceID] = append(clientStorage.unreferencedData[referenceID], data)
	c.memoryStorage[clientID] = clientStorage
	return nil
}

func (c *inMemoryCache) IterateUnreferencedData(clientID string, bufferID string, rmFn func(proto.Message) bool) error {
	clientStorage, clientExists := c.memoryStorage[clientID]
	if !clientExists {
		return fmt.Errorf("clientID '%s' does not exist", clientID)
	}
	data, refExists := clientStorage.unreferencedData[bufferID]
	if !refExists || len(data) == 0 {
		return nil
	}

	// Filter and remove items based on rmFn
	kept := []proto.Message{}
	for _, item := range data {
		if !rmFn(item) {
			kept = append(kept, item)
		}
	}
	clientStorage.unreferencedData[bufferID] = kept
	c.memoryStorage[clientID] = clientStorage

	return nil
}

func (c *inMemoryCache) Close() error {
	return nil
}
