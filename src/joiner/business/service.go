package business

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

var datasetNames = map[int32]string{
	0: "menu_items",
	1: "stores",
	2: "users",
}

const datasetTypeUsers = 2
const separatorBatchData = ","
const registeredAtColumn = 3

type Joiner struct {
	config *config.Config
	m      middleware.MessageMiddleware
}

func NewJoiner(config *config.Config) *Joiner {
	return &Joiner{
		config: config,
	}
}

func (j *Joiner) StartRefConsumer(referenceDatasetQueue string) error {
	m, queueErr := middleware.NewQueueMiddleware(j.config.GatewayAddress, referenceDatasetQueue)
	if queueErr != nil {
		return fmt.Errorf("failed to start queue middleware: %w", queueErr)
	}
	j.m = m

	e := j.m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			j.cacheReferenceData(msg)
		}
		d <- nil
	})

	if int(e) != 0 {
		return fmt.Errorf("StartConsuming returned error code %d", int(e))
	}

	return nil
}

func (j *Joiner) cacheReferenceData(referenceData amqp.Delivery) {
	var batch protocol.ReferenceBatch
	if err := proto.Unmarshal(referenceData.Body, &batch); err != nil {
		_ = referenceData.Nack(false, false)
		return
	}

	datasetFilename, ok := getDatasetFilename(j.config.StorePath, &batch)
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

func (j *Joiner) Stop() error {
	if j.m != nil {
		if err := j.m.StopConsuming(); err == middleware.MessageMiddlewareMessageError {
			return fmt.Errorf("failed to stop consuming")
		}
		if err := j.m.Close(); err == middleware.MessageMiddlewareMessageError {
			return fmt.Errorf("failed to close middleware")
		}
	}
	return nil
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
