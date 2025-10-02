package cache

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"google.golang.org/protobuf/proto"
)

type StoreDataPaths map[enum.TaskType][]string

type DataBatchStore struct {
	storePath      string
	storeDataPaths StoreDataPaths
	openedFile     *os.File
}

func NewCacheStore(storePath string, filename string) *DataBatchStore {
	datasetFilename := filepath.Join(storePath, fmt.Sprintf("%s.pb", filename))

	file, openErr := os.OpenFile(datasetFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if openErr != nil {
		return nil
	}

	return &DataBatchStore{
		storePath:      storePath,
		storeDataPaths: make(StoreDataPaths),
		openedFile:     file,
	}
}

func (cacheStore *DataBatchStore) StoreData(data *data_batch.DataBatch) error {
	rawBytes, marshalErr := proto.Marshal(data)
	if marshalErr != nil {
		return marshalErr
	}

	length := uint32(len(rawBytes))
	if writeLenErr := binary.Write(cacheStore.openedFile, binary.LittleEndian, length); writeLenErr != nil {
		return writeLenErr
	}

	if _, writeDataErr := cacheStore.openedFile.Write(rawBytes); writeDataErr != nil {
		return writeDataErr
	}

	return cacheStore.openedFile.Sync()
}

func (cacheStore *DataBatchStore) ResetStore() error {
	return cacheStore.openedFile.Close()
}
