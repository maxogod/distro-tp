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
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"google.golang.org/protobuf/proto"
)

type StoreDataPaths map[enum.TaskType][]string

type ReferenceDatasetStore struct {
	storePath      string
	storeDataPaths StoreDataPaths
}

type MergeFunc[T proto.Message] func(accumulated, incoming T) T
type KeyFunc[T proto.Message] func(item T) string
type MapJoinMostPurchasesUser map[string]*joined.JoinMostPurchasesUser
type MapJoinStoreTPV map[string]*joined.JoinStoreTPV
type SendAggTask3 func(items MapJoinStoreTPV) error
type SendAggTask4 func(items MapJoinMostPurchasesUser) error

func NewCacheStore(storePath string) *ReferenceDatasetStore {
	return &ReferenceDatasetStore{
		storePath:      storePath,
		storeDataPaths: make(StoreDataPaths),
	}
}

func (refStore *ReferenceDatasetStore) storeData(batch *data_batch.DataBatch, taskType enum.TaskType, filename string) error {
	datasetFilename := filepath.Join(refStore.storePath, fmt.Sprintf("%s.pb", filename))

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

	refStore.updateStorePaths(taskType, datasetFilename)

	return f.Sync()
}

func (refStore *ReferenceDatasetStore) StoreDataBestSelling(batch *data_batch.DataBatch) error {
	return refStore.storeData(batch, enum.T2, "task2_1")
}

func (refStore *ReferenceDatasetStore) StoreDataMostProfits(batch *data_batch.DataBatch) error {
	return refStore.storeData(batch, enum.T2, "task2_2")
}

func (refStore *ReferenceDatasetStore) StoreDataTask3(batch *data_batch.DataBatch) error {
	return refStore.storeData(batch, enum.T3, "task3")
}

func (refStore *ReferenceDatasetStore) StoreDataTask4(batch *data_batch.DataBatch) error {
	return refStore.storeData(batch, enum.T4, "task4")
}

func (refStore *ReferenceDatasetStore) updateStorePaths(taskType enum.TaskType, datasetFilename string) {
	if paths, exists := refStore.storeDataPaths[taskType]; exists {
		for _, p := range paths {
			if p == datasetFilename {
				return
			}
		}
		refStore.storeDataPaths[taskType] = append(paths, datasetFilename)
	} else {
		refStore.storeDataPaths[taskType] = []string{datasetFilename}
	}
}

func (refStore *ReferenceDatasetStore) ResetStore() {
	for _, paths := range refStore.storeDataPaths {
		for _, p := range paths {
			_ = os.Remove(p)
		}
	}

	refStore.storeDataPaths = make(StoreDataPaths)
}

func aggregateData[T proto.Message, B proto.Message](
	datasetName, storePath string,
	createSpecificBatch func() B,
	getItems func(B) []T,
	merge MergeFunc[T],
	key KeyFunc[T],
) (map[string]T, error) {
	filename := fmt.Sprintf("%s/%s.pb", storePath, datasetName)
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	aggregatedItems := make(map[string]T)

	for {
		var length uint32
		if err = binary.Read(f, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		dataBatchBytes := make([]byte, length)
		if _, err = io.ReadFull(f, dataBatchBytes); err != nil {
			return nil, err
		}

		dataBatch := &data_batch.DataBatch{}
		err = proto.Unmarshal(dataBatchBytes, dataBatch)
		if err != nil {
			return nil, err
		}

		specificBatch := createSpecificBatch()
		if err = proto.Unmarshal(dataBatch.Payload, specificBatch); err != nil {
			return nil, err
		}

		for _, item := range getItems(specificBatch) {
			k := key(item)
			if existing, ok := aggregatedItems[k]; ok {
				aggregatedItems[k] = merge(existing, item)
			} else {
				aggregatedItems[k] = proto.Clone(item).(T)
			}
		}
	}

	_ = os.Remove(filename)

	return aggregatedItems, nil
}

func (refStore *ReferenceDatasetStore) AggregateDataTask2(gatewayControllerDataQueue string) error {
	return nil
}

func (refStore *ReferenceDatasetStore) AggregateDataTask3(gatewayControllerDataQueue string, sendAggDataTask3 SendAggTask3) error {
	aggregatedData, err := aggregateData(
		"task3",
		refStore.storePath,
		func() *joined.JoinStoreTPVBatch {
			return &joined.JoinStoreTPVBatch{}
		},
		func(batch *joined.JoinStoreTPVBatch) []*joined.JoinStoreTPV {
			return batch.Items
		},
		func(accumulated, incoming *joined.JoinStoreTPV) *joined.JoinStoreTPV {
			accumulated.Tpv += incoming.Tpv
			return accumulated
		},
		func(item *joined.JoinStoreTPV) string {
			return item.YearHalfCreatedAt + "|" + item.StoreName
		},
	)

	if err != nil {
		return err
	}

	return sendAggDataTask3(aggregatedData)
}

func (refStore *ReferenceDatasetStore) AggregateDataTask4(gatewayControllerDataQueue string, sendAggDataTask4 SendAggTask4) error {
	aggregatedData, err := aggregateData(
		"task4",
		refStore.storePath,
		func() *joined.JoinMostPurchasesUserBatch {
			return &joined.JoinMostPurchasesUserBatch{}
		},
		func(batch *joined.JoinMostPurchasesUserBatch) []*joined.JoinMostPurchasesUser {
			return batch.Users
		},
		func(accumulated, incoming *joined.JoinMostPurchasesUser) *joined.JoinMostPurchasesUser {
			accumulated.PurchasesQty += incoming.PurchasesQty
			return accumulated
		},
		func(item *joined.JoinMostPurchasesUser) string {
			return item.StoreName + "|" + item.UserBirthdate
		},
	)

	if err != nil {
		return err
	}

	top3 := top3ByStore(aggregatedData)

	return sendAggDataTask4(top3)
}

func top3ByStore(data MapJoinMostPurchasesUser) MapJoinMostPurchasesUser {
	usersByStore := make(map[string][]*joined.JoinMostPurchasesUser)
	for _, item := range data {
		usersByStore[item.StoreName] = append(usersByStore[item.StoreName], item)
	}

	result := make(map[string]*joined.JoinMostPurchasesUser)

	for _, users := range usersByStore {
		sort.Slice(users, func(i, j int) bool {
			if users[i].PurchasesQty == users[j].PurchasesQty {
				return users[i].UserBirthdate < users[j].UserBirthdate
			}
			return users[i].PurchasesQty > users[j].PurchasesQty
		})

		limit := 3
		if len(users) < 3 {
			limit = len(users)
		}

		for _, user := range users[:limit] {
			k := user.StoreName + "|" + user.UserBirthdate
			result[k] = user
		}
	}

	return result
}
