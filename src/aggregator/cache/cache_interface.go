package cache

import "google.golang.org/protobuf/proto"

// The CacheService interface defines the methods that any cache implementation must provide.
// This allows the business logic to interact with different caching mechanisms
// without being tightly coupled to a specific implementation.
type CacheService interface {
	StoreAggregatedData(cacheReference string, dataKey string, data proto.Message, joinFunction func(existing, new proto.Message) (proto.Message, error)) error
	StoreBatch(cacheReference string, data []proto.Message) error
	SortData(cacheReference string, sortFn func(a, b proto.Message) bool) error
	ReadBatch(cacheReference string, amount int32) ([]proto.Message, error)
	Close() error
}
