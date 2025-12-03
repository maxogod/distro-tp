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
const TEMP_FILE_SUFFIX = "TEMP%"

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
	c.stopWritingInternal(cacheReference)
}

func (c *diskMemoryStorage) StopWritingTemp(cacheReference string) {
	c.stopWritingInternal(TEMP_FILE_SUFFIX + cacheReference)
}

func (c *diskMemoryStorage) FlushWriting(cacheReference string) error {
	fileName := c.getFileName(cacheReference)
	val, exists := c.storageChannels.Load(fileName)
	if !exists {
		return fmt.Errorf("[Write] no active writer for cache reference: %s", cacheReference)
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
		return nil, fmt.Errorf("[Read] no active writer for cache reference: %s", cacheReference)
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

func (c *diskMemoryStorage) SaveTempFile(cacheReference string) error {
	tempFileName := FOLDER_PATH + TEMP_FILE_SUFFIX + cacheReference + CACHE_EXTENSION
	finalFileName := c.getFileName(cacheReference)

	if c.fileHandler.IsFilePresent(finalFileName) {
		logger.Logger.Debug("Final file %s already exists, skipping rename from temp file %s", finalFileName, tempFileName)
		return nil
	}

	if !c.fileHandler.IsFilePresent(tempFileName) {
		return fmt.Errorf("temp file %s does not exist", tempFileName)
	}

	err := c.fileHandler.RenameFile(tempFileName, finalFileName)
	if err != nil {
		logger.Logger.Errorf("Error renaming temp file %s to %s: %v", tempFileName, finalFileName, err)
		return err
	}
	return nil
}

func (c *diskMemoryStorage) RemoveAllTempFiles() error {
	tempFiles, err := filepath.Glob(FOLDER_PATH + TEMP_FILE_SUFFIX + "*" + CACHE_EXTENSION)
	if err != nil {
		logger.Logger.Errorf("Error globbing temp files: %v", err)
		return err
	}
	for _, file := range tempFiles {
		c.fileHandler.DeleteFile(file)
	}
	return nil
}

// ============ Private methods ===============

func (c *diskMemoryStorage) getFileName(cacheReference string) string {
	return FOLDER_PATH + cacheReference + CACHE_EXTENSION
}

// Private helper method to handle the common logic
func (c *diskMemoryStorage) stopWritingInternal(cacheReference string) {
	fileName := c.getFileName(cacheReference)
	val, exists := c.storageChannels.Load(fileName)
	if !exists {
		logger.Logger.Warnf("[Stop] no active writer for cache reference: %s", cacheReference)
		return
	}
	fileWriter := val.(*filehandler.FileWriter)
	fileWriter.FinishWriting()
	c.storageChannels.Delete(fileName)
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
	tempCacheReference := TEMP_FILE_SUFFIX + cacheReference
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
