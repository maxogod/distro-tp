package task_executor

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/business/file_service"
	"github.com/maxogod/distro-tp/src/gateway/internal/utils"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type taskExecutor struct {
	dataPath string
	conn     *network.ConnectionInterface
}

func NewTaskExecutor(dataPath string, conn *network.ConnectionInterface) TaskExecutor {
	return &taskExecutor{
		dataPath: dataPath,
		conn:     conn,
	}
}

func (t *taskExecutor) Task1() error {
	fs := file_service.NewFileService[*raw.Transaction](100)
	ch := make(chan []*raw.Transaction)

	csv_file_path := t.dataPath + "/transactions/transactions_202407.csv"
	go fs.ReadAsBatches(csv_file_path, ch, utils.TransactionFromRecord)

	for batch := range ch {
		tBatch := raw.TransactionBatch{
			Transactions: batch,
		}
		data, err := proto.Marshal(&tBatch)
		if err != nil {
			log.Errorf("failed to marshal transaction batch: %v", err)
			continue
		}

		err = t.conn.SendData(data)
		if err != nil {
			log.Errorf("failed to send transaction batch: %v", err)
		}
	}

	return nil
}

func (t *taskExecutor) Task2() error {
	return nil
}

func (t *taskExecutor) Task3() error {
	return nil
}

func (t *taskExecutor) Task4() error {
	return nil
}
