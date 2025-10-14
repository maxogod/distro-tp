package cache

import (
	"google.golang.org/protobuf/proto"
)

// The CacheService interface defines the methods that any cache implementation must provide.
// This allows the business logic to interact with different caching mechanisms
// without being tightly coupled to a specific implementation.
type CacheService interface {
	// StoreBatch stores a batch of proto.Message items associated with the given clientID.
	StoreRefData(clientID string, referenceID string, data proto.Message) error

	// GetRefData retrieves the data associated with the given clientID and referenceID.
	// It returns the data, and an error.
	GetRefData(clientID string, referenceID string) (proto.Message, error)

	// BufferUnreferencedData buffers data that does not have an associated reference ID yet.
	BufferUnreferencedData(clientID string, referenceID string, data proto.Message) error

	// IterateUnreferencedData iterates over unreferenced data for a given clientID and referenceID,
	// applying the provided removal function to each item.
	// This should be used when the reference ID becomes available to process and possibly remove the buffered data.
	IterateUnreferencedData(clientID string, referenceID string, rmFn func(proto.Message) bool) error

	// RemoveRefData removes all reference data associated with the given clientID.
	RemoveRefData(clientID string)

	// Close performs any necessary cleanup for the cache service.
	Close() error
}
