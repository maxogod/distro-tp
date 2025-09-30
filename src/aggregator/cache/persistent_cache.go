package cache

import (
	"os"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
)

type ReferenceDatasetStore struct {
	storePath         string
	pathAggregateData string
}

func NewCacheStore(storePath string) *ReferenceDatasetStore {
	return &ReferenceDatasetStore{
		storePath: storePath,
	}
}

func (refStore *ReferenceDatasetStore) StoreReferenceData(batch *data_batch.DataBatch, datasetName string) error {
	return nil
}

func (refStore *ReferenceDatasetStore) ResetStore() {
	_ = os.Remove(refStore.pathAggregateData)
	refStore.pathAggregateData = ""
}
