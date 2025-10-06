package cache

import (
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
)

type StoreDataPaths map[enum.TaskType][]string

type DataBatchStore struct {
	batches []*data_batch.DataBatch
}

func NewCacheStore() *DataBatchStore {
	return &DataBatchStore{
		batches: []*data_batch.DataBatch{},
	}
}

func (cacheStore *DataBatchStore) StoreData(data *data_batch.DataBatch) error {
	cacheStore.batches = append(cacheStore.batches, data)
	return nil
}

func (cacheStore *DataBatchStore) ResetStore() error {
	cacheStore.batches = []*data_batch.DataBatch{}
	return nil
}

func (cacheStore *DataBatchStore) GetBatches() []*data_batch.DataBatch {
	return cacheStore.batches
}
