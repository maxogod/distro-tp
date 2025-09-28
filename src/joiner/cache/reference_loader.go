package cache

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/maxogod/distro-tp/src/common/protocol"
	"google.golang.org/protobuf/proto"
)

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
