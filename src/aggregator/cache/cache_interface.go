package cache

import "google.golang.org/protobuf/proto"

// The CacheService interface defines the methods that any cache implementation must provide.
// This allows the business logic to interact with different caching mechanisms
// without being tightly coupled to a specific implementation.
type CacheService interface {
	StoreSortedBatch(cacheReference string, data []*proto.Message, sortFn func(a, b *proto.Message) bool) error
	StoreBatch(cacheReference string, data []*proto.Message) error
	ReadBatch(cacheReference string, amount int32) ([]*proto.Message, error)
	Remove(cacheReference string) error
	Close() error
}
