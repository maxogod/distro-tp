package cache

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	"google.golang.org/protobuf/proto"
)

type RefDatasetNames map[models.RefDatasetType]string
type RefDatasetPaths map[models.RefDatasetType][]string

func defaultReferenceDatasets() RefDatasetNames {
	return RefDatasetNames{
		models.MenuItems: "menu_items",
		models.Stores:    "stores",
		models.Users:     "users",
	}
}

type ReferenceDatasetStore struct {
	datasetNames RefDatasetNames
	refDatasets  RefDatasetPaths
}

func NewCacheStore() *ReferenceDatasetStore {
	return &ReferenceDatasetStore{
		datasetNames: defaultReferenceDatasets(),
		refDatasets:  make(RefDatasetPaths),
	}
}

func (refStore *ReferenceDatasetStore) StoreReferenceData(storePath string, batch *protocol.ReferenceBatch) error {
	datasetFilename, ok := refStore.getDatasetFilename(storePath, batch)
	if !ok {
		return fmt.Errorf("failed to get dataset filename for dataset type: %d", batch.DatasetType)
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

	refStore.updateRefDatasets(batch.DatasetType, datasetFilename)

	return f.Sync()
}

func (refStore *ReferenceDatasetStore) getDatasetFilename(storePath string, batch *protocol.ReferenceBatch) (string, bool) {
	refDatasetType := models.RefDatasetType(batch.DatasetType)
	datasetName, ok := refStore.datasetNames[refDatasetType]
	if !ok {
		return "", false
	}

	var datasetFilename string

	if refDatasetType == models.Users {
		users := &protocol.Users{}
		if err := proto.Unmarshal(batch.Payload, users); err != nil {
			return "", false
		}
		user := users.Users[0]

		year, month, err := getYearMonth(user.RegisteredAt)
		if err != nil {
			return "", false
		}
		datasetFilename = filepath.Join(storePath, fmt.Sprintf("%s_%d%s.pb", datasetName, year, month))
	} else {
		datasetFilename = filepath.Join(storePath, fmt.Sprintf("%s.pb", datasetName))
	}

	return datasetFilename, ok
}

func (refStore *ReferenceDatasetStore) updateRefDatasets(datasetType int32, datasetFilename string) {
	if paths, exists := refStore.refDatasets[models.RefDatasetType(datasetType)]; exists {
		for _, p := range paths {
			if p == datasetFilename {
				return
			}
		}
		refStore.refDatasets[models.RefDatasetType(datasetType)] = append(paths, datasetFilename)
	} else {
		refStore.refDatasets[models.RefDatasetType(datasetType)] = []string{datasetFilename}
	}
}

func getYearMonth(userRegisteredAt string) (int, string, error) {
	t, err := time.Parse(time.DateTime, userRegisteredAt)
	if err != nil {
		return 0, "", err
	}

	return t.Year(), fmt.Sprintf("%02d", int(t.Month())), nil
}
