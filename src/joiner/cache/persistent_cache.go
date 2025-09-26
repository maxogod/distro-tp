package cache

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/joiner/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

var datasetNames = map[int32]string{
	0: "menu_items",
	1: "stores",
	2: "users",
}

const datasetTypeUsers = 2
const separatorBatchData = ","
const registeredAtColumn = 3

func StoreReferenceData(storePath string, referenceData *amqp.Delivery, batch *protocol.ReferenceBatch) {
	datasetFilename, ok := getDatasetFilename(storePath, batch)
	if !ok {
		_ = referenceData.Nack(false, false)
		return
	}

	if err := utils.AppendToCSVFile(datasetFilename, batch.Payload); err != nil {
		_ = referenceData.Nack(false, true)
		return
	}

	_ = referenceData.Ack(false)
}

func getDatasetFilename(storePath string, batch *protocol.ReferenceBatch) (string, bool) {
	datasetName, ok := datasetNames[batch.DatasetType]
	if !ok {
		return "", false
	}

	var datasetFilename string

	if batch.DatasetType == datasetTypeUsers {
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
