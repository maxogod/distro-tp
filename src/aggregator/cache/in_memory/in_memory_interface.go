package in_memory

type InMemoryCache interface {
	StoreData(cacheReference string, data [][]byte) error
	FlushAllData(flushFn func(cacheReference string, data [][]byte) error) error
	RemoveCache(cacheReference string) error
	Close() error
}
