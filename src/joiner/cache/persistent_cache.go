package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	"google.golang.org/protobuf/proto"
)

var datasetNames = map[models.RefDatasetType]string{
	models.MenuItems: "menu_items",
	models.Stores:    "stores",
	models.Users:     "users",
}

func StoreReferenceData(storePath string, batch *protocol.ReferenceBatch) error {
	datasetFilename, ok := getDatasetFilename(storePath, batch)
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

	return f.Sync()
}

func getDatasetFilename(storePath string, batch *protocol.ReferenceBatch) (string, bool) {
	refDatasetType := models.RefDatasetType(batch.DatasetType)
	datasetName, ok := datasetNames[refDatasetType]
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

func getYearMonth(userRegisteredAt string) (int, string, error) {
	t, err := time.Parse(time.DateTime, userRegisteredAt)
	if err != nil {
		return 0, "", err
	}

	return t.Year(), fmt.Sprintf("%02d", int(t.Month())), nil
}

func LoadReferenceData[T proto.Message, B proto.Message](
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

		refBatch := &protocol.ReferenceBatch{}
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

func LoadStores(path string) (map[int32]*protocol.Store, error) {
	stores, err := LoadReferenceData(
		path,
		func() *protocol.Stores { return &protocol.Stores{} },
		func(batch *protocol.Stores) []*protocol.Store { return batch.Stores },
	)
	if err != nil {
		return nil, err
	}

	storesMap := make(map[int32]*protocol.Store)
	for _, store := range stores {
		storesMap[store.StoreID] = store
	}
	return storesMap, nil
}

func LoadMenuItems(path string) (map[int32]*protocol.MenuItem, error) {
	menuItems, err := LoadReferenceData(
		path,
		func() *protocol.MenuItems { return &protocol.MenuItems{} },
		func(batch *protocol.MenuItems) []*protocol.MenuItem { return batch.Items },
	)
	if err != nil {
		return nil, err
	}

	menuItemsMap := make(map[int32]*protocol.MenuItem)
	for _, menuItem := range menuItems {
		menuItemsMap[menuItem.ItemId] = menuItem
	}
	return menuItemsMap, nil
}

func createDataBatchFromJoined[T proto.Message](taskType int32, items []T, makeDataBatchMsg func([]T) proto.Message) (*protocol.DataBatch, error) {
	batch := makeDataBatchMsg(items)

	payloadBytes, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	return &protocol.DataBatch{
		TaskType: taskType,
		Payload:  payloadBytes,
	}, nil
}

func CreateJoinStoreTPVBatch(taskType int32, joined []*protocol.JoinStoreTPV) (*protocol.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joined,
		func(items []*protocol.JoinStoreTPV) proto.Message {
			return &protocol.JoinStoreTPVBatch{Items: items}
		},
	)
}

func CreateBestSellingBatch(taskType int32, joined []*protocol.JoinBestSellingProducts) (*protocol.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joined,
		func(items []*protocol.JoinBestSellingProducts) proto.Message {
			return &protocol.JoinBestSellingProductsBatch{Items: items}
		},
	)
}

func CreateMostProfitsBatch(taskType int32, joined []*protocol.JoinMostProfitsProducts) (*protocol.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joined,
		func(items []*protocol.JoinMostProfitsProducts) proto.Message {
			return &protocol.JoinMostProfitsProductsBatch{Items: items}
		},
	)
}
