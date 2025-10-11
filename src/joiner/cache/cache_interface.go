package cache

import "google.golang.org/protobuf/proto"

// The CacheService interface defines the methods that any cache implementation must provide.
// This allows the business logic to interact with different caching mechanisms
// without being tightly coupled to a specific implementation.
type CacheService interface {
	StoreRefData(cacheReference string, data *proto.Message) error
	ReadRefData(cacheReference string, amount int32) ([]*proto.Message, error)

	StoreBatch(cacheReference string, data []*proto.Message) error
	ReadBatch(cacheReference string, amount int32) ([]*proto.Message, error)

	Close() error
}
