package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/maxogod/distro-tp/src/common/models/raw"
	"google.golang.org/protobuf/proto"
)

const (
	mainDataset = 0
)

type StoresMap map[int32]*raw.Store
type MenuItemsMap map[int32]*raw.MenuItem
type UsersMap map[int32]*raw.User

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

func (refStore *ReferenceDatasetStore) LoadUsers(userIds []int) (UsersMap, error) {
	sort.Ints(userIds)

	idsSet := make(map[int]struct{}, len(userIds))
	for _, id := range userIds {
		idsSet[id] = struct{}{}
	}

	files, openErr := os.ReadDir(refStore.storePath)
	if openErr != nil {
		return nil, fmt.Errorf("failed to read dir %s: %w", refStore.storePath, openErr)
	}

	datasetRanges := make(map[int][2]int)
	for i, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "users_") {
			usersDatasetPath := filepath.Join(refStore.storePath, f.Name())
			firstId, lastId, err := getUserIdRangeFromDatasetName(usersDatasetPath)
			if err != nil {
				return nil, err
			}
			datasetRanges[i] = [2]int{firstId, lastId}
		}
	}

	datasetsToLoad := make([]int, 0)
	for i, rng := range datasetRanges {
		const firstId = 0
		const lastId = 1
		if intersects(rng[firstId], rng[lastId], userIds) {
			datasetsToLoad = append(datasetsToLoad, i)
		}
	}

	usersMap := make(UsersMap)
	for _, idx := range datasetsToLoad {
		//users, err := loadUsersFile(
		//	refStore.refDatasets[enum.Users][idx],
		//	func() *raw.UserBatch { return &raw.UserBatch{} },
		//	func(batch *raw.UserBatch) []*raw.User { return batch.Users },
		//)
		users, err := loadUsersFile()
		if err != nil {
			return nil, err
		}

		for _, user := range users {
			if _, ok := idsSet[int(user.UserId)]; ok {
				usersMap[user.UserId] = user
			}
		}
	}

	return usersMap, nil
}

func loadUsersFile(userFilePath string) ([]*raw.User, error) {
	f, openErr := os.Open(userFilePath)
	if openErr != nil {
		return nil, openErr
	}
	defer f.Close()

	var users []*raw.User
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

		items := userBatch.Users
		users = append(users, items...)
	}

	return users, nil
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

func intersects(firstId, lastId int, userIds []int) bool {
	i := sort.SearchInts(userIds, firstId)
	return i < len(userIds) && userIds[i] <= lastId
}
