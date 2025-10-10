package cache

import (
	"container/heap"
	"fmt"

	"google.golang.org/protobuf/proto"
)

type storage struct {
	sortedData   heap.Interface
	unsortedData []*proto.Message
	index        int
}

// This CacheService implementation provides fast in-memory storage of data.
// It supports both sorted and unsorted data storage using a min-heap and a FIFO queue respectively.
// Note: This implementation is not reliable since data is lost if the service restarts.
type InMemoryCache struct {
	memoryStorage map[string]storage
}

func NewInMemoryCache() CacheService {
	return &InMemoryCache{
		memoryStorage: make(map[string]storage),
	}
}

func (c *InMemoryCache) StoreSortedBatch(cacheReference string, data []*proto.Message, sortFn func(a, b *proto.Message) bool) error {
	storageData, exists := c.memoryStorage[cacheReference]

	if !exists {
		// first time this reference is seen, create its heap with sort function
		h := NewMessageHeap(sortFn)
		heap.Init(h)
		storageData = storage{sortedData: h}
	}

	// Push data to the heap
	for _, msg := range data {
		heap.Push(storageData.sortedData, msg)
	}

	// Update the storage
	c.memoryStorage[cacheReference] = storageData
	return nil
}

func (c *InMemoryCache) StoreBatch(cacheReference string, data []*proto.Message) error {
	storageData, exists := c.memoryStorage[cacheReference]

	if !exists {
		storageData = storage{
			unsortedData: []*proto.Message{},
			index:        0,
		}
	}

	// Enqueue data
	storageData.unsortedData = append(storageData.unsortedData, data...)

	c.memoryStorage[cacheReference] = storageData
	return nil
}

func (c *InMemoryCache) ReadBatch(cacheReference string, amount int32) ([]*proto.Message, error) {
	storageData, exists := c.memoryStorage[cacheReference]
	if !exists {
		return nil, fmt.Errorf("no data found for cache reference: %s", cacheReference)
	}

	// Decide which private method to call
	if storageData.sortedData != nil && storageData.sortedData.Len() > 0 {
		return c.readSortedBatch(cacheReference, amount)
	} else if storageData.index < len(storageData.unsortedData) {
		return c.readUnorderedBatch(cacheReference, amount)
	}

	return nil, nil
}

// readSortedBatch pops `amount` items from the sorted heap (private)
func (c *InMemoryCache) readSortedBatch(cacheReference string, amount int32) ([]*proto.Message, error) {
	storageData, exists := c.memoryStorage[cacheReference]
	if !exists || storageData.sortedData == nil || storageData.sortedData.Len() == 0 {
		return nil, nil
	}

	results := make([]*proto.Message, 0, amount)
	for i := int32(0); i < amount && storageData.sortedData.Len() > 0; i++ {
		msg := heap.Pop(storageData.sortedData).(*proto.Message)
		results = append(results, msg)
	}

	c.memoryStorage[cacheReference] = storageData
	return results, nil
}

// readUnorderedBatch dequeues `amount` items from the FIFO queue (private)
func (c *InMemoryCache) readUnorderedBatch(cacheReference string, amount int32) ([]*proto.Message, error) {
	storageData, exists := c.memoryStorage[cacheReference]
	if !exists || storageData.index >= len(storageData.unsortedData) {
		return nil, nil
	}

	start := storageData.index
	end := min(start+int(amount), len(storageData.unsortedData))

	results := storageData.unsortedData[start:end]
	storageData.index = end

	c.memoryStorage[cacheReference] = storageData
	return results, nil
}

func (c *InMemoryCache) Remove(cacheReference string) error {
	_, exists := c.memoryStorage[cacheReference]
	if !exists {
		return fmt.Errorf("no data found for cache reference: %s", cacheReference)
	}

	delete(c.memoryStorage, cacheReference)
	return nil
}

func (c *InMemoryCache) Close() error {
	c.memoryStorage = make(map[string]storage)
	return nil
}
