package cache

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	"github.com/maxogod/distro-tp/src/common/utils"
	jProtocol "github.com/maxogod/distro-tp/src/joiner/protocol"
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

	if err := utils.AppendToCSVFile(datasetFilename, batch.Payload); err != nil {
		return err
	}

	return nil
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
		datasetFilename = filepath.Join(storePath, fmt.Sprintf("%s_%d%s.csv", datasetName, year, month))
	} else {
		datasetFilename = filepath.Join(storePath, fmt.Sprintf("%s.csv", datasetName))
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

func LoadStores(path string) (map[int32]*protocol.Store, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	csvStores, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	storesMap := make(map[int32]*protocol.Store)
	for _, row := range csvStores {
		if len(row) < 8 {
			continue
		}

		storeID, _ := strconv.Atoi(row[0])
		postalCode, _ := strconv.Atoi(row[3])
		latitude, _ := strconv.ParseFloat(row[6], 64)
		longitude, _ := strconv.ParseFloat(row[7], 64)

		storesMap[int32(storeID)] = &protocol.Store{
			StoreID:    int32(storeID),
			StoreName:  row[1],
			Street:     row[2],
			PostalCode: int32(postalCode),
			City:       row[4],
			State:      row[5],
			Latitude:   latitude,
			Longitude:  longitude,
		}
	}

	return storesMap, nil
}

func CreateDataBatchFromJoined(taskType int32, joined []*jProtocol.JoinStoreTPV) (*protocol.DataBatch, error) {
	batch := &jProtocol.JoinStoreTPVBatch{
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
