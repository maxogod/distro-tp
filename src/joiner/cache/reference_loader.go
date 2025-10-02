package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/maxogod/distro-tp/src/common/models/raw"
	"google.golang.org/protobuf/proto"
)

const (
	mainDataset = 0
)

type StoresMap map[int32]*raw.Store
type MenuItemsMap map[int32]*raw.MenuItem

func (refStore *ReferenceDatasetStore) LoadStores() (StoresMap, error) {
	datasetName := "stores"
	datasetFilename := filepath.Join(refStore.storePath, fmt.Sprintf("%s.pb", datasetName))

	f, openErr := os.Open(datasetFilename)
	if openErr != nil {
		return nil, openErr
	}
	defer f.Close()

	var stores []*raw.Store
	for {
		var length uint32
		if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		storeBatchBytes := make([]byte, length)
		if _, err := io.ReadFull(f, storeBatchBytes); err != nil {
			return nil, err
		}

		storeBatch := &raw.StoreBatch{}
		err := proto.Unmarshal(storeBatchBytes, storeBatch)
		if err != nil {
			return nil, err
		}

		items := storeBatch.Stores
		stores = append(stores, items...)
	}

	storesMap := make(map[int32]*raw.Store)
	for _, store := range stores {
		storesMap[store.StoreId] = store
	}
	return storesMap, nil
}

func (refStore *ReferenceDatasetStore) LoadMenuItems() (MenuItemsMap, error) {
	datasetName := "menu_items"
	datasetFilename := filepath.Join(refStore.storePath, fmt.Sprintf("%s.pb", datasetName))

	f, openErr := os.Open(datasetFilename)
	if openErr != nil {
		return nil, openErr
	}
	defer f.Close()

	var menuItems []*raw.MenuItem
	for {
		var length uint32
		if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		menuItemsBatchBytes := make([]byte, length)
		if _, err := io.ReadFull(f, menuItemsBatchBytes); err != nil {
			return nil, err
		}

		menuItemBatch := &raw.MenuItemBatch{}
		err := proto.Unmarshal(menuItemsBatchBytes, menuItemBatch)
		if err != nil {
			return nil, err
		}

		items := menuItemBatch.MenuItems
		menuItems = append(menuItems, items...)
	}

	menuItemsMap := make(map[int32]*raw.MenuItem)
	for _, menuItem := range menuItems {
		menuItemsMap[menuItem.ItemId] = menuItem
	}
	return menuItemsMap, nil
}

func (refStore *ReferenceDatasetStore) LoadUser(userId int32) (*raw.User, error) {
	files, openErr := os.ReadDir(refStore.storePath)
	if openErr != nil {
		return nil, fmt.Errorf("failed to read dir %s: %w", refStore.storePath, openErr)
	}

	var datasetToLoad string
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "users_") {
			usersDatasetPath := filepath.Join(refStore.storePath, f.Name())
			firstId, lastId, err := getUserIdRangeFromDatasetName(usersDatasetPath)
			if err != nil {
				return nil, err
			}
			if int(userId) >= firstId && int(userId) <= lastId {
				datasetToLoad = usersDatasetPath
				break
			}
		}
	}

	return loadUsersFile(datasetToLoad, userId)
}

func loadUsersFile(userFilePath string, userId int32) (*raw.User, error) {
	f, openErr := os.Open(userFilePath)
	if openErr != nil {
		return nil, openErr
	}
	defer f.Close()

	for {
		var length uint32
		if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		userBatchBytes := make([]byte, length)
		if _, err := io.ReadFull(f, userBatchBytes); err != nil {
			return nil, err
		}

		userBatch := &raw.UserBatch{}
		err := proto.Unmarshal(userBatchBytes, userBatch)
		if err != nil {
			return nil, err
		}

		for _, user := range userBatch.Users {
			if user.UserId == userId {
				return user, nil
			}
		}
	}

	return nil, fmt.Errorf("user with id %d not found in file %s", userId, userFilePath)
}

func getUserIdRangeFromDatasetName(usersDatasetPath string) (int, int, error) {
	var firstUserId, lastUserId int

	usersDatasetName := filepath.Base(usersDatasetPath)

	_, err := fmt.Sscanf(usersDatasetName, "users_%d-%d.pb", &firstUserId, &lastUserId)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid users dataset name: %s", usersDatasetName)
	}
	return firstUserId, lastUserId, nil
}
