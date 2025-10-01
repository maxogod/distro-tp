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

type ReferenceDatasetStore struct {
	storePath      string
	storeDataPaths StoreDataPaths
}

func NewCacheStore(storePath string) *ReferenceDatasetStore {
	return &ReferenceDatasetStore{
		storePath:      storePath,
		storeDataPaths: make(StoreDataPaths),
	}
}

func (refStore *ReferenceDatasetStore) storeData(batch *data_batch.DataBatch, taskType enum.TaskType, filename string) error {
	datasetFilename := filepath.Join(refStore.storePath, fmt.Sprintf("%s.pb", filename))

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

	refStore.updateStorePaths(taskType, datasetFilename)

	return f.Sync()
}

func (refStore *ReferenceDatasetStore) StoreDataBestSelling(batch *data_batch.DataBatch) error {
	return refStore.storeData(batch, enum.T2, "task2_1")
}

func (refStore *ReferenceDatasetStore) StoreDataMostProfits(batch *data_batch.DataBatch) error {
	return refStore.storeData(batch, enum.T2, "task2_2")
}

func (refStore *ReferenceDatasetStore) StoreDataTask3(batch *data_batch.DataBatch) error {
	return refStore.storeData(batch, enum.T3, "task3")
}

func (refStore *ReferenceDatasetStore) StoreDataTask4(batch *data_batch.DataBatch) error {
	return refStore.storeData(batch, enum.T4, "task4")
}

func (refStore *ReferenceDatasetStore) updateStorePaths(taskType enum.TaskType, datasetFilename string) {
	if paths, exists := refStore.storeDataPaths[taskType]; exists {
		for _, p := range paths {
			if p == datasetFilename {
				return
			}
		}
		refStore.storeDataPaths[taskType] = append(paths, datasetFilename)
	} else {
		refStore.storeDataPaths[taskType] = []string{datasetFilename}
	}
}

func (refStore *ReferenceDatasetStore) ResetStore() {
	for _, paths := range refStore.storeDataPaths {
		for _, p := range paths {
			_ = os.Remove(p)
		}
	}

	refStore.storeDataPaths = make(StoreDataPaths)
}
