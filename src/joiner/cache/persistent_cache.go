package cache

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

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
	storePath    string
}

func NewCacheStore(storePath string) *ReferenceDatasetStore {
	return &ReferenceDatasetStore{
		datasetTypes: defaultReferenceDatasets(),
		storePath:    storePath,
	}
}

func (refStore *ReferenceDatasetStore) PersistMenuItemsBatch(batch *raw.MenuItemBatch) error {
	datasetName := "menu_items"
	datasetFilename := filepath.Join(refStore.storePath, fmt.Sprintf("%s.pb", datasetName))

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

	return f.Sync()
}

func (refStore *ReferenceDatasetStore) PersistStoresBatch(batch *raw.StoreBatch) error {
	datasetName := "stores"
	datasetFilename := filepath.Join(refStore.storePath, fmt.Sprintf("%s.pb", datasetName))

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

	return f.Sync()
}

func (refStore *ReferenceDatasetStore) PersistUsersBatch(batch *raw.UserBatch) error {
	datasetName := "users"
	datasetFilename := refStore.getUserDatasetFilename(batch.Users, datasetName)

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

	return f.Sync()
}

func (refStore *ReferenceDatasetStore) getUserDatasetFilename(users []*raw.User, datasetName string) string {
	firstUser := users[0]
	lastUser := users[len(users)-1]
	return filepath.Join(refStore.storePath, fmt.Sprintf("%s_%d-%d.pb", datasetName, firstUser.UserId, lastUser.UserId))
}

func (refStore *ReferenceDatasetStore) ResetStore() error {
	entries, err := os.ReadDir(refStore.storePath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		path := filepath.Join(refStore.storePath, entry.Name())

		err = os.RemoveAll(path)
		if err != nil {
			return err
		}
	}

	return nil
}
