package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

const separatorBatchData = ","
const registeredAtColumn = 3

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
		year, month, err := getYearMonth(batch.Payload)
		if err != nil {
			return "", false
		}
		datasetFilename = filepath.Join(storePath, fmt.Sprintf("%s_%d%s.pb", datasetName, year, month))
	} else {
		datasetFilename = filepath.Join(storePath, fmt.Sprintf("%s.pb", datasetName))
	}

	return datasetFilename, ok
}

func getYearMonth(batchPayload []byte) (int, string, error) {
	row := string(batchPayload)
	cols := strings.Split(row, separatorBatchData)

	dateStr := strings.TrimSpace(cols[registeredAtColumn])

	t, err := time.Parse(time.DateTime, dateStr)
	if err != nil {
		return 0, "", err
	}

	return t.Year(), fmt.Sprintf("%02d", int(t.Month())), nil
}

func LoadReferenceData[T proto.Message](path string, createRefDatasetProto func() T) ([]T, error) {
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

		refDatasetMsg := createRefDatasetProto()
		if err := proto.Unmarshal(refDatasetBytes, refDatasetMsg); err != nil {
			return nil, err
		}

		referenceDataset = append(referenceDataset, refDatasetMsg)
	}

	return referenceDataset, nil
}

func LoadStores(path string) (map[int32]*protocol.Store, error) {
	stores, err := LoadReferenceData(path, func() *protocol.Store { return &protocol.Store{} })
	if err != nil {
		return nil, err
	}

	storesMap := make(map[int32]*protocol.Store)
	for _, store := range stores {
		storesMap[store.StoreID] = store
	}
	return storesMap, nil
}

func CreateDataBatchFromJoined(taskType int32, joined []*protocol.JoinStoreTPV) (*protocol.DataBatch, error) {
	batch := &protocol.JoinStoreTPVBatch{
		Items: joined,
	}

	payloadBytes, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	dataBatch := &protocol.DataBatch{
		TaskType: taskType,
		Payload:  payloadBytes,
	}

	return dataBatch, nil
}
