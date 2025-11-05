package cache

import (
	"sort"

	filehandler "github.com/maxogod/distro-tp/src/common/utils/file_handler"
)

const CACHE_EXTENSION = ".cache"
const SEPERATOR = "@"

// This is a simple implementation of a CacheService that uses both disk for storage.
type diskMemoryCache struct {
	fileHandler     filehandler.FileHandler
	storageChannels map[string]chan []byte
}

func NewDiskMemoryCache() CacheService {
	return &diskMemoryCache{
		fileHandler:     filehandler.NewFileHandler(),
		storageChannels: make(map[string]chan []byte),
	}
}

func (c *diskMemoryCache) StoreAggregatedData(cacheReference string, dataKey string, joinFunction func(existingBytes *[]byte)) error {
	fileName := cacheReference + CACHE_EXTENSION
	err := c.fileHandler.SaveIndexedData(fileName, dataKey, joinFunction)
	if err != nil {
		return err
	}
	return nil
}

func (c *diskMemoryCache) StoreData(cacheReference string, data [][]byte) error {
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
		c.fileHandler.SaveData(fileName, storageCh)
	}()
	c.storageChannels[fileName] = storageCh
	for _, entry := range data {
		storageCh <- entry
	}

	return nil
}

func (c *diskMemoryCache) ReadData(cacheReference string, read_ch chan []byte) {

	fileName := cacheReference + CACHE_EXTENSION
	store_ch := c.storageChannels[fileName]
	if store_ch != nil {
		close(store_ch)
		delete(c.storageChannels, fileName)
	}
	go c.fileHandler.ReadData(fileName, read_ch)

}

func (c *diskMemoryCache) SortData(cacheReference string, sortFunction func(a, b []byte) bool) error {

	fileName := cacheReference + CACHE_EXTENSION

	store_ch := c.storageChannels[fileName]
	if store_ch != nil {
		close(store_ch)
		delete(c.storageChannels, fileName)
	}

	sort_ch := make(chan []byte)

	go c.fileHandler.ReadData(fileName, sort_ch)

	allData := [][]byte{}
	for entry := range sort_ch {
		allData = append(allData, entry)
	}

	sort.Slice(allData, func(i, j int) bool {
		return sortFunction(allData[i], allData[j])
	})

	save_ch := make(chan []byte)
	go c.fileHandler.SaveData(fileName, save_ch)

	for _, entry := range allData {
		save_ch <- entry
	}
	close(save_ch)

	return nil
}

func (c *diskMemoryCache) RemoveCache(cacheReference string) error {
	fileName := cacheReference + CACHE_EXTENSION
	c.fileHandler.CloseFile(fileName)
	c.fileHandler.DeleteFile(fileName)
	return nil
}

func (c *diskMemoryCache) Close() error {

	for _, storageCh := range c.storageChannels {
		close(storageCh)
	}

	c.fileHandler.Close()
	return nil
}
