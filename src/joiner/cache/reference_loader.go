package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"google.golang.org/protobuf/proto"
)

const (
	mainDataset = 0
)

type StoresMap map[int32]*raw.Store
type MenuItemsMap map[int32]*raw.MenuItem
type UsersMap map[int32]*raw.User

func loadReferenceData[T proto.Message, B proto.Message](
	path string,
	createSpecificBatch func() B,
	extractItems func(B) []T,
) ([]T, error) {
	f, openErr := os.Open(path)
	if openErr != nil {
		return nil, openErr
	}
	defer f.Close()

	var referenceDataset []T
	for {
		var length uint32
		if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		refDatasetBytes := make([]byte, length)
		if _, err := io.ReadFull(f, refDatasetBytes); err != nil {
			return nil, err
		}

		refBatch := &data_batch.DataBatch{}
		err := proto.Unmarshal(refDatasetBytes, refBatch)
		if err != nil {
			return nil, err
		}

		specificBatch := createSpecificBatch()
		if err = proto.Unmarshal(refBatch.Payload, specificBatch); err != nil {
			return nil, err
		}

		items := extractItems(specificBatch)
		referenceDataset = append(referenceDataset, items...)
	}

	return referenceDataset, nil
}

func (refStore *ReferenceDatasetStore) LoadStores() (StoresMap, error) {
	stores, err := loadReferenceData(
		refStore.refDatasets[enum.Stores][mainDataset],
		func() *raw.StoreBatch { return &raw.StoreBatch{} },
		func(batch *raw.StoreBatch) []*raw.Store { return batch.Stores },
	)
	if err != nil {
		return nil, err
	}

	storesMap := make(map[int32]*raw.Store)
	for _, store := range stores {
		storesMap[store.StoreId] = store
	}
	return storesMap, nil
}

func (refStore *ReferenceDatasetStore) LoadMenuItems() (MenuItemsMap, error) {
	menuItems, err := loadReferenceData(
		refStore.refDatasets[enum.MenuItems][mainDataset],
		func() *raw.MenuItemBatch { return &raw.MenuItemBatch{} },
		func(batch *raw.MenuItemBatch) []*raw.MenuItem { return batch.MenuItems },
	)
	if err != nil {
		return nil, err
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

	datasetRanges := make(map[int][2]int)
	for i, usersDatasetPath := range refStore.refDatasets[enum.Users] {
		firstId, lastId, err := getUserIdRangeFromDatasetName(usersDatasetPath)
		if err != nil {
			return nil, err
		}
		datasetRanges[i] = [2]int{firstId, lastId}
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
		users, err := loadReferenceData(
			refStore.refDatasets[enum.Users][idx],
			func() *raw.UserBatch { return &raw.UserBatch{} },
			func(batch *raw.UserBatch) []*raw.User { return batch.Users },
		)
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
