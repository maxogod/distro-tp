package cache

import (
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/logger"
	filehandler "github.com/maxogod/distro-tp/src/common/utils/file_handler"
	cache "github.com/maxogod/distro-tp/src/common/worker/storage"
	"google.golang.org/protobuf/proto"
)

const CACHE_EXTENSION = ".cache"

// This is a simple implementation of a CacheService that uses both disk for storage.
type diskMemoryStorage struct {
	fileHandler     filehandler.FileHandler
	storageChannels map[string]chan []byte
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
		if err := c.fileHandler.WriteData(fileName, storageCh); err != nil {
			logger.Logger.Warn("The file [%s] was deleted when writing: %v", fileName, err)
		}

	}()
	c.storageChannels[fileName] = storageCh
	for _, entry := range data {
		storageCh <- entry
	}

	return nil
}

func (c *diskMemoryStorage) ReadData(cacheReference string, read_ch chan []byte) {

	fileName := cacheReference + CACHE_EXTENSION
	c.fileHandler.ReadData(fileName, read_ch)

}

func (c *diskMemoryStorage) RemoveCache(cacheReference string) error {
	// Find all files starting with cacheReference
	files, err := filepath.Glob(cacheReference + "*")
	if err != nil {
		logger.Logger.Errorf("Error globbing files for %s: %v", cacheReference, err)
		return err
	}
	_, exists := c.storageChannels[cacheReference+CACHE_EXTENSION]
	if exists {
		delete(c.storageChannels, cacheReference+CACHE_EXTENSION)
	}
	// Delete each matching file
	for _, file := range files {
		c.fileHandler.DeleteFile(file)
	}
	return nil
}

func (c *diskMemoryStorage) Close() error {

	c.fileHandler.Close()
	return nil
}

// ============ Helper methods ================

func StoreBatch[T proto.Message](cs cache.StorageService, cacheReference string, data []T) error {
	listBytes := make([][]byte, len(data))
	for i := range data {
		bytes, err := proto.Marshal(data[i])
		if err != nil {
			logger.Logger.Errorf("Error marshalling proto message: %v", err)
			return err
		}
		listBytes[i] = bytes
	}
	err := cs.StoreData(cacheReference, listBytes)
	if err != nil {
		logger.Logger.Errorf("Error storing data for client [%s]: %v", cacheReference, err)
	}
	return nil
}
