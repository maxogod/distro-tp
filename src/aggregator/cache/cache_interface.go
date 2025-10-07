package cache

import "google.golang.org/protobuf/proto"

// TODO: document this :D
type CacheService interface {
	StoreSortedBatch(cacheReference string, data []*proto.Message, sortFn func(a, b *proto.Message) bool) error
	StoreBatch(cacheReference string, data []*proto.Message) error
	ReadBatch(cacheReference string, amount int32) ([]*proto.Message, error)
	Remove(cacheReference string) error
	Close() error
}
