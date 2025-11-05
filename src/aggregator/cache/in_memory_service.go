package cache

// import (
// 	"fmt"
// 	"sort"

// 	"google.golang.org/protobuf/proto"
// )

// // This struct holds the data for each cache reference.
// // Aggregated data represents, data that must be acumulated or joined via an ID or reference
// // List data represents, data that can be used as it is without any extra transformations required
// type storage struct {
// 	mappedData map[string]proto.Message
// 	listData   []proto.Message
// 	sortedData []proto.Message
// 	index      int
// }

// // This CacheService implementation provides fast in-memory storage of data.
// // It supports both sorted and unsorted data storage using a min-heap and a FIFO queue respectively.
// // Note: This implementation is not reliable since data is lost if the service restarts.
// type inMemoryCache struct {
// 	memoryStorage map[string]storage
// }

// func NewInMemoryCache() CacheService {
// 	return &inMemoryCache{
// 		memoryStorage: make(map[string]storage),
// 	}
// }

// // TODO: WARNINIG THIS MIGHT NOT WORK ANYMORE, BUT IT DOESNT MATTER BECAUSE
// // WE ARE NOW MORE FOCUSED ON STORING ON DISK INSTED OF MEMORY
// func (c *inMemoryCache) StoreAggregatedData(cacheReference string, dataKey string, joinFunction func(existingBytes *[]byte)) error {
// 	storageData, exists := c.memoryStorage[cacheReference]
// 	if !exists {
// 		storageData = storage{
// 			mappedData: make(map[string]proto.Message),
// 			index:      0,
// 		}
// 	}

// 	existing, exists := storageData.mappedData[dataKey]
// 	if exists { // If the key exists, use the join function to combine the data
// 		existingBytes, _ := proto.Marshal(existing)
// 		joinFunction(&existingBytes)
// 		joinedData := proto.Clone(existing)
// 		storageData.mappedData[dataKey] = joinedData
// 	} else {
// 		newData := []byte{}
// 		joinFunction(&newData)
// 		data := proto.Clone(existing)
// 		storageData.mappedData[dataKey] = data
// 	}

// 	c.memoryStorage[cacheReference] = storageData
// 	return nil
// }

// func (c *inMemoryCache) StoreBatch(store_ch chan proto.Message, cacheReference string, data []proto.Message) error {
// 	storageData, exists := c.memoryStorage[cacheReference]

// 	if !exists {
// 		storageData = storage{
// 			listData: []proto.Message{},
// 			index:    0,
// 		}
// 	}

// 	storageData.listData = append(storageData.listData, data...)

// 	c.memoryStorage[cacheReference] = storageData
// 	return nil
// }

// func (c *inMemoryCache) ReadBatch(read_ch chan proto.Message, cacheReference string, amount int32) ([]proto.Message, error) {
// 	storageData, exists := c.memoryStorage[cacheReference]
// 	if !exists {
// 		return nil, fmt.Errorf("no data found for cache reference: %s", cacheReference)
// 	}

// 	// This only occures when having aggregated data that was never sorted
// 	// In this case we convert the map to a slice for easier reading
// 	if storageData.sortedData == nil && storageData.mappedData != nil {
// 		sortedData := make([]proto.Message, 0, len(storageData.mappedData))
// 		for _, msg := range storageData.mappedData {
// 			sortedData = append(sortedData, msg)
// 		}
// 		storageData.sortedData = sortedData
// 		c.memoryStorage[cacheReference] = storageData
// 	}

// 	// Decide which private method to call
// 	if storageData.sortedData != nil {
// 		return c.readBatch(cacheReference, amount, storageData.sortedData)
// 	} else if storageData.listData != nil {
// 		return c.readBatch(cacheReference, amount, storageData.listData)
// 	}

// 	return nil, nil
// }

// func (c *inMemoryCache) readBatch(cacheReference string, amount int32, data []proto.Message) ([]proto.Message, error) {
// 	storageData, exists := c.memoryStorage[cacheReference]
// 	if !exists || storageData.index >= len(data) {
// 		return nil, nil
// 	}

// 	start := storageData.index
// 	end := min(start+int(amount), len(data))

// 	results := data[start:end]
// 	storageData.index = end

// 	c.memoryStorage[cacheReference] = storageData
// 	return results, nil
// }

// func (c *inMemoryCache) SortData(cacheReference string, sortFn func(a, b proto.Message) bool) error {
// 	storageData, exists := c.memoryStorage[cacheReference]
// 	if !exists || storageData.mappedData == nil {
// 		return fmt.Errorf("no data found for cache reference: %s", cacheReference)
// 	}
// 	if len(storageData.mappedData) == 0 {
// 		return fmt.Errorf("no data to sort for cache reference: %s", cacheReference)
// 	}
// 	// Collect values from mappedData into a slice
// 	sortedData := make([]proto.Message, 0, len(storageData.mappedData))
// 	for _, msg := range storageData.mappedData {
// 		sortedData = append(sortedData, msg)
// 	}
// 	// Sort the slice using the provided sort function
// 	sort.Slice(sortedData, func(i, j int) bool {
// 		return sortFn(sortedData[i], sortedData[j])
// 	})
// 	storageData.sortedData = sortedData
// 	c.memoryStorage[cacheReference] = storageData
// 	return nil
// }

// func (c *inMemoryCache) RemoveCache(cacheReference string) error {
// 	delete(c.memoryStorage, cacheReference)
// 	return nil
// }

// func (c *inMemoryCache) Close() error {
// 	c.memoryStorage = make(map[string]storage)
// 	return nil
// }
