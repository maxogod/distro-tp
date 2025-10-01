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

func (cacheStore *DataBatchStore) storeData(batch *data_batch.DataBatch, taskType enum.TaskType, filename string) error {
	datasetFilename := filepath.Join(cacheStore.storePath, fmt.Sprintf("%s.pb", filename))

	data, protoErr := proto.Marshal(batch)
	if protoErr != nil {
		return protoErr
	}

	f, openErr := os.OpenFile(datasetFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if openErr != nil {
		return openErr
	}
	defer f.Close()

	length := uint32(len(data))
	if writeLenErr := binary.Write(f, binary.LittleEndian, length); writeLenErr != nil {
		return writeLenErr
	}

	if _, writeDataErr := f.Write(data); writeDataErr != nil {
		return writeDataErr
	}

	cacheStore.updateStorePaths(taskType, datasetFilename)

	return f.Sync()
}

func (cacheStore *DataBatchStore) StoreDataBestSelling(batch *data_batch.DataBatch) error {
	return cacheStore.storeData(batch, enum.T2, "task2_1")
}

func (cacheStore *DataBatchStore) StoreDataMostProfits(batch *data_batch.DataBatch) error {
	return cacheStore.storeData(batch, enum.T2, "task2_2")
}

func (cacheStore *DataBatchStore) StoreDataTask3(batch *data_batch.DataBatch) error {
	return cacheStore.storeData(batch, enum.T3, "task3")
}

func (cacheStore *DataBatchStore) StoreDataTask4(batch *data_batch.DataBatch) error {
	return cacheStore.storeData(batch, enum.T4, "task4")
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
