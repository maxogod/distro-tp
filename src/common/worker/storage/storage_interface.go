package storage

// The StorageService interface defines the methods that any cache implementation must provide.
// This allows the business logic to interact with different caching mechanisms
// without being tightly coupled to a specific implementation.
type StorageService interface {

	// StartWriting begins writing data to the cache identified by cacheReference.
	StartWriting(cacheReference string, data [][]byte) error
	// StopWriting stops writing data to the cache identified by cacheReference.
	StopWriting(cacheReference string)
	// FlushWriting flushes any buffered data to the cache identified by cacheReference.
	FlushWriting(cacheReference string) error
	// ReadAllData reads all data from the cache identified by cacheReference.
	ReadAllData(cacheReference string) (chan []byte, error)
	// ReadData reads the data from the specific point it was called
	// If called while StartWriting is initiated, it should read the data written until that moment
	ReadData(cacheReference string) (chan []byte, error)
	// RemoveCache removes the cache identified by cacheReference.
	RemoveCache(cacheReference string) error
	// GetAllFilesReferences returns a list of all file references currently stored in the cache.
	GetAllFilesReferences() []string
	// SaveTempFile renames the temporary file to a permanent cache file and returns its reference.
	SaveTempFile(cacheReference string) error
	// RemoveAllTempFiles removes all temporary files from the storage.
	RemoveAllTempFiles() error
	// Close closes the storage service, releasing any resources.
	Close() error
}
