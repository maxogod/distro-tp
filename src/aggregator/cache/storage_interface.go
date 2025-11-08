package cache

// The StorageService interface defines the methods that any cache implementation must provide.
// This allows the business logic to interact with different caching mechanisms
// without being tightly coupled to a specific implementation.
type StorageService interface {
	StoreData(cacheReference string, data [][]byte) error
	ReadData(cacheReference string, read_ch chan []byte)
	RemoveCache(cacheReference string) error
	Close() error
}
