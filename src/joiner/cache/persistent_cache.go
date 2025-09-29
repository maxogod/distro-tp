package cache

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"google.golang.org/protobuf/proto"
)

type RefDatasetTypes map[string]enum.RefDatasetType
type RefDatasetPaths map[enum.RefDatasetType][]string

func defaultReferenceDatasets() RefDatasetTypes {
	return RefDatasetTypes{
		"menu_items": enum.MenuItems,
		"stores":     enum.Stores,
		"users":      enum.Users,
	}
}

type ReferenceDatasetStore struct {
	datasetTypes RefDatasetTypes
	refDatasets  RefDatasetPaths
	storePath    string
}

func NewCacheStore(storePath string) *ReferenceDatasetStore {
	return &ReferenceDatasetStore{
		datasetTypes: defaultReferenceDatasets(),
		refDatasets:  make(RefDatasetPaths),
		storePath:    storePath,
	}
}

func (refStore *ReferenceDatasetStore) StoreReferenceData(batch *data_batch.DataBatch, datasetName string) error {
	datasetFilename, ok := refStore.getDatasetFilename(batch, datasetName)
	if !ok {
		return fmt.Errorf("failed to get dataset filename for dataset: %s", datasetName)
	}

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

	refStore.updateRefDatasets(datasetName, datasetFilename)

	return f.Sync()
}

func (refStore *ReferenceDatasetStore) getDatasetFilename(batch *data_batch.DataBatch, datasetName string) (string, bool) {
	var datasetFilename string

	if refStore.datasetTypes[datasetName] == enum.Users {
		users := &raw.UserBatch{}
		if err := proto.Unmarshal(batch.Payload, users); err != nil {
			return "", false
		}
		firstUser := users.Users[0]
		lastUser := users.Users[len(users.Users)-1]

		datasetFilename = filepath.Join(refStore.storePath, fmt.Sprintf("%s_%d-%d.pb", datasetName, firstUser.UserId, lastUser.UserId))
	} else {
		datasetFilename = filepath.Join(refStore.storePath, fmt.Sprintf("%s.pb", datasetName))
	}

	return datasetFilename, true
}

func (refStore *ReferenceDatasetStore) updateRefDatasets(datasetName string, datasetFilename string) {
	datasetType := refStore.datasetTypes[datasetName]
	if paths, exists := refStore.refDatasets[datasetType]; exists {
		for _, p := range paths {
			if p == datasetFilename {
				return
			}
		}
		refStore.refDatasets[datasetType] = append(paths, datasetFilename)
	} else {
		refStore.refDatasets[datasetType] = []string{datasetFilename}
	}
}
