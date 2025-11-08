package in_memory

// This struct holds the data for each cache reference.
// Aggregated data represents, data that must be acumulated or joined via an ID or reference
// List data represents, data that can be used as it is without any extra transformations required
type storage struct {
	// flattenedData map[string][]byte
	listData [][]byte
}

// This CacheService implementation provides fast in-memory storage of data.
// Note: This implementation is not reliable since data is lost if the service restarts.
type inMemoryCache struct {
	memoryStorage map[string]storage
}

func NewInMemoryCache() InMemoryCache {
	return inMemoryCache{
		memoryStorage: make(map[string]storage),
	}
}

func (c inMemoryCache) StoreData(cacheReference string, data [][]byte) error {
	storageData, exists := c.memoryStorage[cacheReference]

	if !exists {
		storageData = storage{
			listData: [][]byte{},
		}
	}
	storageData.listData = append(storageData.listData, data...)
	c.memoryStorage[cacheReference] = storageData
	return nil
}

func (c inMemoryCache) FlushAllData(flushFn func(cacheReference string, data [][]byte) error) error {
	for cacheReference, storage := range c.memoryStorage {
		err := flushFn(cacheReference, storage.listData)
		if err != nil {
			return err
		}
		delete(c.memoryStorage, cacheReference)
	}
	return nil
}

func (c inMemoryCache) RemoveCache(cacheReference string) error {
	delete(c.memoryStorage, cacheReference)
	return nil
}

func (c inMemoryCache) Close() error {
	return nil
}
