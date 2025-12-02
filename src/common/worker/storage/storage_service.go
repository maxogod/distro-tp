package storage

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/maxogod/distro-tp/src/common/logger"
	filehandler "github.com/maxogod/distro-tp/src/common/utils/file_handler"
	"google.golang.org/protobuf/proto"
)

const FOLDER_PATH = "storage/"
const CACHE_EXTENSION = ".cache"
const TEMP_FILE_SUFFIX = "_temp"

type diskMemoryStorage struct {
	fileHandler     filehandler.FileHandler
	storageChannels sync.Map // map[string]*filehandler.FileWriter
}

func NewDiskMemoryStorage() StorageService {
	return &diskMemoryStorage{
		fileHandler: filehandler.NewFileHandler(),
	}
}

func (c *diskMemoryStorage) StartWriting(cacheReference string, data [][]byte) error {
	fileName := c.getFileName(cacheReference)

	val, exists := c.storageChannels.Load(fileName)
	var fileWriter *filehandler.FileWriter

	if !exists {
		newFileWriter, err := c.fileHandler.InitWriter(fileName)
		if err != nil {
			logger.Logger.Errorf("Error initializing file writer for %s: %v", fileName, err)
			return err
		}
		fileWriter = newFileWriter
		c.storageChannels.Store(fileName, newFileWriter)
	} else {
		fileWriter = val.(*filehandler.FileWriter)
	}

	for _, item := range data {
		fileWriter.Write(item)
	}
	return nil
}

func (c *diskMemoryStorage) StopWriting(cacheReference string) {
	fileName := c.getFileName(cacheReference)
	val, exists := c.storageChannels.Load(fileName)
	if !exists {
		logger.Logger.Warnf("No active writer for cache reference: %s", cacheReference)
		return
	}
	fileWriter := val.(*filehandler.FileWriter)
	fileWriter.FinishWriting()
	c.storageChannels.Delete(fileName)
}

func (c *diskMemoryStorage) FlushWriting(cacheReference string) error {
	fileName := c.getFileName(cacheReference)
	val, exists := c.storageChannels.Load(fileName)
	if !exists {
		return fmt.Errorf("no active writer for cache reference: %s", cacheReference)
	}
	fileWriter := val.(*filehandler.FileWriter)
	fileWriter.Sync()
	return nil
}

func (c *diskMemoryStorage) ReadAllData(cacheReference string) (chan []byte, error) {
	fileName := c.getFileName(cacheReference)
	return c.fileHandler.InitReader(fileName)
}

func (c *diskMemoryStorage) ReadData(cacheReference string) (chan []byte, error) {
	fileName := c.getFileName(cacheReference)

	val, exists := c.storageChannels.Load(fileName)
	if !exists {
		return nil, fmt.Errorf("no active writer for cache reference: %s", cacheReference)
	}
	fileWriter := val.(*filehandler.FileWriter)
	fileWriter.Sync()

	amountOfLines, err := c.fileHandler.GetFileSize(fileName)
	if err != nil {
		return nil, err
	}
	return c.fileHandler.InitReadUpTo(fileName, amountOfLines)
}

func (c *diskMemoryStorage) GetAllFilesReferences() []string {
	var references []string
	files, err := filepath.Glob(FOLDER_PATH + "*" + CACHE_EXTENSION)
	if err != nil {
		logger.Logger.Errorf("Error globbing cache files: %v", err)
		return references
	}
	for _, file := range files {
		references = append(references, file[len(FOLDER_PATH):len(file)-len(CACHE_EXTENSION)])
	}
	return references
}

func (c *diskMemoryStorage) RemoveCache(cacheReference string) error {
	files, err := filepath.Glob(FOLDER_PATH + cacheReference + "*")
	if err != nil {
		logger.Logger.Errorf("Error globbing files for %s: %v", cacheReference, err)
		return err
	}

	filename := c.getFileName(cacheReference)
	c.storageChannels.Delete(filename)

	for _, file := range files {
		c.fileHandler.DeleteFile(file)
	}
	return nil
}

func (c *diskMemoryStorage) Close() error {
	c.storageChannels.Range(func(key, value any) bool {
		fileWriter := value.(*filehandler.FileWriter)
		fileWriter.FinishWriting()
		c.storageChannels.Delete(key)
		return true
	})
	c.fileHandler.Close()
	return nil
}

func (c *diskMemoryStorage) getFileName(cacheReference string) string {
	return FOLDER_PATH + cacheReference + CACHE_EXTENSION
}

func (c *diskMemoryStorage) SaveTempFile(cacheReference string) string {
	tempFileName := cacheReference + TEMP_FILE_SUFFIX
	finalFileName := c.getFileName(cacheReference)
	err := c.fileHandler.RenameFile(tempFileName, finalFileName)
	if err != nil {
		logger.Logger.Errorf("Error renaming temp file %s to %s: %v", tempFileName, finalFileName, err)
		return ""
	}
	return cacheReference
}

// ============ Helper methods ================

func StoreBatch[T proto.Message](cs StorageService, cacheReference string, data []T) error {
	listBytes := make([][]byte, len(data))
	for i := range data {
		bytes, err := proto.Marshal(data[i])
		if err != nil {
			logger.Logger.Errorf("Error marshalling proto message: %v", err)
			return err
		}
		listBytes[i] = bytes
	}
	err := cs.StartWriting(cacheReference, listBytes)
	if err != nil {
		logger.Logger.Errorf("Error storing data for client [%s]: %v", cacheReference, err)
	}
	return nil
}

func StoreTempBatch[T proto.Message](cs StorageService, cacheReference string, data []T) error {
	tempCacheReference := cacheReference + TEMP_FILE_SUFFIX
	listBytes := make([][]byte, len(data))
	for i := range data {
		bytes, err := proto.Marshal(data[i])
		if err != nil {
			logger.Logger.Errorf("Error marshalling proto message: %v", err)
			return err
		}
		listBytes[i] = bytes
	}
	err := cs.StartWriting(tempCacheReference, listBytes)
	if err != nil {
		logger.Logger.Errorf("Error storing data for client [%s]: %v", tempCacheReference, err)
	}
	return nil
}
