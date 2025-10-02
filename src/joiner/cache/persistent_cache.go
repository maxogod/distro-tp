package cache

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
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

	Users      map[int32]*raw.User
	Menu_items map[int32]*raw.MenuItem
	Stores     map[int32]*raw.Store
}

func NewCacheStore(storePath string) *ReferenceDatasetStore {
	return &ReferenceDatasetStore{
		datasetTypes: defaultReferenceDatasets(),
		storePath:    storePath,
	}
}

func (refStore *ReferenceDatasetStore) PersistMenuItemsBatch(batch *raw.MenuItemBatch) error {
	if refStore.Menu_items == nil {
		refStore.Menu_items = make(map[int32]*raw.MenuItem)
	}

	for _, menuItem := range batch.MenuItems {
		refStore.Menu_items[menuItem.ItemId] = menuItem
	}

	return nil
	// datasetName := "menu_items"
	// datasetFilename := filepath.Join(refStore.storePath, fmt.Sprintf("%s.pb", datasetName))

	// data, protoErr := proto.Marshal(batch)
	// if protoErr != nil {
	// 	return protoErr
	// }

	// f, openErr := os.OpenFile(datasetFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	// if openErr != nil {
	// 	return openErr
	// }
	// defer f.Close()

	// length := uint32(len(data))
	// if writeLenErr := binary.Write(f, binary.LittleEndian, length); writeLenErr != nil {
	// 	return writeLenErr
	// }

	// if _, writeDataErr := f.Write(data); writeDataErr != nil {
	// 	return writeDataErr
	// }

	// return f.Sync()
}

func (refStore *ReferenceDatasetStore) PersistStoresBatch(batch *raw.StoreBatch) error {

	if refStore.Stores == nil {
		refStore.Stores = make(map[int32]*raw.Store)
	}

	for _, store := range batch.Stores {
		refStore.Stores[store.StoreId] = store
	}

	return nil
	// datasetName := "stores"
	// datasetFilename := filepath.Join(refStore.storePath, fmt.Sprintf("%s.pb", datasetName))

	// data, protoErr := proto.Marshal(batch)
	// if protoErr != nil {
	// 	return protoErr
	// }

	// f, openErr := os.OpenFile(datasetFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	// if openErr != nil {
	// 	return openErr
	// }
	// defer f.Close()

	// length := uint32(len(data))
	// if writeLenErr := binary.Write(f, binary.LittleEndian, length); writeLenErr != nil {
	// 	return writeLenErr
	// }

	// if _, writeDataErr := f.Write(data); writeDataErr != nil {
	// 	return writeDataErr
	// }

	// return f.Sync()
}

func (refStore *ReferenceDatasetStore) PersistUsersBatch(batch *raw.UserBatch) error {
	if refStore.Users == nil {
		refStore.Users = make(map[int32]*raw.User)
	}

	for _, user := range batch.Users {
		refStore.Users[user.UserId] = user
	}

	return nil
	// datasetName := "users"
	// datasetFilename := refStore.getUserDatasetFilename(batch.Users, datasetName)

	// data, protoErr := proto.Marshal(batch)
	// if protoErr != nil {
	// 	return protoErr
	// }

	// f, openErr := os.OpenFile(datasetFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	// if openErr != nil {
	// 	return openErr
	// }
	// defer f.Close()

	// length := uint32(len(data))
	// if writeLenErr := binary.Write(f, binary.LittleEndian, length); writeLenErr != nil {
	// 	return writeLenErr
	// }

	// if _, writeDataErr := f.Write(data); writeDataErr != nil {
	// 	return writeDataErr
	// }

	// return f.Sync()
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
