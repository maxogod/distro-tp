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
}

func NewCacheStore(storePath string) *DataBatchStore {
	return &DataBatchStore{
		storePath:      storePath,
		storeDataPaths: make(StoreDataPaths),
	}
}

func (cacheStore *DataBatchStore) storeData(data *data_batch.DataBatch, taskType enum.TaskType, filename string) error {
	datasetFilename := filepath.Join(cacheStore.storePath, fmt.Sprintf("%s.pb", filename))

	f, openErr := os.OpenFile(datasetFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if openErr != nil {
		return openErr
	}
	defer f.Close()

	rawBytes, marshalErr := proto.Marshal(data)
	if marshalErr != nil {
		return marshalErr
	}

	length := uint32(len(rawBytes))
	if writeLenErr := binary.Write(f, binary.LittleEndian, length); writeLenErr != nil {
		return writeLenErr
	}

	if _, writeDataErr := f.Write(rawBytes); writeDataErr != nil {
		return writeDataErr
	}

	cacheStore.updateStorePaths(taskType, datasetFilename)

	return f.Sync()
}

func (cacheStore *DataBatchStore) StoreDataTask1(data *data_batch.DataBatch) error {
	return cacheStore.storeData(data, enum.T1, "task1")
}

func (cacheStore *DataBatchStore) StoreDataBestSelling(data *data_batch.DataBatch) error {
	return cacheStore.storeData(data, enum.T2, "task2_1")
}

func (cacheStore *DataBatchStore) StoreDataMostProfits(data *data_batch.DataBatch) error {
	return cacheStore.storeData(data, enum.T2, "task2_2")
}

func (cacheStore *DataBatchStore) StoreDataTask3(data *data_batch.DataBatch) error {
	return cacheStore.storeData(data, enum.T3, "task3")
}

func (cacheStore *DataBatchStore) StoreDataTask4(data *data_batch.DataBatch) error {
	return cacheStore.storeData(data, enum.T4, "task4")
}

func (cacheStore *DataBatchStore) updateStorePaths(taskType enum.TaskType, datasetFilename string) {
	if paths, exists := cacheStore.storeDataPaths[taskType]; exists {
		for _, p := range paths {
			if p == datasetFilename {
				return
			}
		}
		cacheStore.storeDataPaths[taskType] = append(paths, datasetFilename)
	} else {
		cacheStore.storeDataPaths[taskType] = []string{datasetFilename}
	}
}

func (cacheStore *DataBatchStore) ResetStore() {
	for _, paths := range cacheStore.storeDataPaths {
		for _, p := range paths {
			_ = os.Remove(p)
		}
	}

	cacheStore.storeDataPaths = make(StoreDataPaths)
}
