package cache

import (
	"github.com/maxogod/distro-tp/src/aggregator/cache"
	filehandler "github.com/maxogod/distro-tp/src/common/utils/file_handler"
)

const CACHE_EXTENSION = ".cache"
const SEPERATOR = "@"
const DONE_BYTE = 0xff

// This is a simple implementation of a CacheService that uses both disk for storage.
type diskMemoryStorage struct {
	fileHandler     filehandler.FileHandler
	storageChannels map[string]chan []byte
	doneChannels    map[string]chan string
}

func NewDiskMemoryStorage() cache.StorageService {
	return &diskMemoryStorage{
		fileHandler:     filehandler.NewFileHandler(),
		storageChannels: make(map[string]chan []byte),
	}
}

func (c *diskMemoryStorage) StoreData(cacheReference string, data [][]byte) error {
	// this is the unique filename where the clients aggregated data will remain
	fileName := cacheReference + CACHE_EXTENSION

	// if the storage is already initialized, then we store it here
	if storageCh, exists := c.storageChannels[fileName]; exists {
		for _, entry := range data {
			storageCh <- entry
		}
		return nil
	}

	// else, we initialize the storage channel and start the goroutine to store the data
	storageCh := make(chan []byte)
	go func() {
		c.fileHandler.WriteData(fileName, storageCh)
	}()
	c.storageChannels[fileName] = storageCh
	for _, entry := range data {
		storageCh <- entry
	}

	return nil
}

func (c *diskMemoryStorage) FinishData(cacheReference string) {
	fileName := cacheReference + CACHE_EXTENSION
	store_ch := c.storageChannels[fileName]
	if store_ch != nil {
		close(store_ch)
		delete(c.storageChannels, fileName)
	}
}

func (c *diskMemoryStorage) ReadData(cacheReference string, read_ch chan []byte) {

	fileName := cacheReference + CACHE_EXTENSION
	go c.fileHandler.ReadData(fileName, read_ch)

}

func (c *diskMemoryStorage) RemoveCache(cacheReference string) error {
	fileName := cacheReference + CACHE_EXTENSION
	c.fileHandler.DeleteFile(fileName)
	return nil
}

func (c *diskMemoryStorage) Close() error {

	c.fileHandler.Close()
	return nil
}
