package cache

// The CacheService interface defines the methods that any cache implementation must provide.
// This allows the business logic to interact with different caching mechanisms
// without being tightly coupled to a specific implementation.
type CacheService interface {
	//StoreAggregatedData(cacheReference string, dataKey string, data proto.Message, joinFunction func(existing, new proto.Message) (proto.Message, error)) error
	StoreAggregatedData(cacheReference string, dataKey string, joinFunction func(existingBytes *[]byte)) error
	StoreData(cacheReference string, data [][]byte) error
	SortData(cacheReference string, sortFunction func(a, b []byte) bool) error
	ReadData(cacheReference string, read_ch chan []byte)
	Close() error
	RemoveCache(cacheReference string) error
}
