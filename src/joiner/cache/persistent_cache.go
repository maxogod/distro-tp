package cache

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	"github.com/maxogod/distro-tp/src/common/utils"
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
