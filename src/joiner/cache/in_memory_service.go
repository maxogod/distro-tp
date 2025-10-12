package cache

import (
	"google.golang.org/protobuf/proto"
)

// This struct holds the data for each cache reference.
// Aggregated data represents, data that must be acumulated or joined via an ID or reference
// List data represents, data that can be used as it is without any extra transformations required
type storage struct {
	mappedData map[string]*proto.Message
	listData   []*proto.Message
	sortedData []*proto.Message
	index      int
}

// This CacheService implementation provides fast in-memory storage of data.
// It supports both sorted and unsorted data storage using a min-heap and a FIFO queue respectively.
// Note: This implementation is not reliable since data is lost if the service restarts.
type inMemoryCache struct {
	memoryStorage map[string]storage
}

func NewInMemoryCache() CacheService {
	return &inMemoryCache{
		memoryStorage: make(map[string]storage),
	}
}

func (c *inMemoryCache) StoreRefData(cacheReference string, data proto.Message) error {
	return nil
}

func (c *inMemoryCache) ReadRefData(cacheReference string, amount int32) ([]proto.Message, error) {
	return nil, nil
}

func (c *inMemoryCache) StoreBatch(cacheReference string, data []proto.Message) error {
	return nil
}

func (c *inMemoryCache) ReadBatch(cacheReference string, amount int32) ([]proto.Message, error) {
	return nil, nil
}

func (c *inMemoryCache) RemoveRefData(cacheReference string) error {
	return nil
}

func (c *inMemoryCache) Close() error {
	return nil
}
