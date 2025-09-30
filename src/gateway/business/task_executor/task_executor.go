package task_executor

import (
	"os"
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
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
	transactionsDir := t.dataPath + TransactionsDirPath
	err := readAndSendData(
		t.conn,
		enum.T1,
		transactionsDir,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	return nil
}

func (t *taskExecutor) Task2() error {
	menuItemsDir := t.dataPath + MenuItemsDirPath
	err := readAndSendData(
		t.conn,
		enum.T2,
		menuItemsDir,
		utils.MenuItemFromRecord,
		utils.MenuItemBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send menu items data: %v", err)
		return err
	}

	transactionsItemsDir := t.dataPath + TransactionItemsDirPath
	err = readAndSendData(
		t.conn,
		enum.T2,
		transactionsItemsDir,
		utils.TransactionItemsFromRecord,
		utils.TransactionItemsBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transaction items data: %v", err)
		return err
	}

	return nil
}

func (t *taskExecutor) Task3() error {
	storesDir := t.dataPath + StoresDirPath
	err := readAndSendData(
		t.conn,
		enum.T3,
		storesDir,
		utils.StoreFromRecord,
		utils.StoreBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send stores data: %v", err)
		return err
	}

	transactionsDir := t.dataPath + TransactionsDirPath
	err = readAndSendData(
		t.conn,
		enum.T3,
		transactionsDir,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	return nil
}

func (t *taskExecutor) Task4() error {
	usersDir := t.dataPath + UsersDirPath
	err := readAndSendData(
		t.conn,
		enum.T4,
		usersDir,
		utils.UserFromRecord,
		utils.UserBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send users data: %v", err)
		return err
	}

	storesDir := t.dataPath + StoresDirPath
	err = readAndSendData(
		t.conn,
		enum.T4,
		storesDir,
		utils.StoreFromRecord,
		utils.StoreBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send stores data: %v", err)
		return err
	}

	transactionsDir := t.dataPath + TransactionsDirPath
	err = readAndSendData(
		t.conn,
		enum.T4,
		transactionsDir,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	return nil
}

func readAndSendData[T any](
	conn *network.ConnectionInterface,
	taskType enum.TaskType,
	dataDir string,
	fromRecordFunc func([]string) T,
	makeBatchFunc func([]T) []byte,
) error {
	fs := file_service.NewFileService[T](100)
	ch := make(chan []T)

	files, err := os.ReadDir(dataDir)
	if err != nil {
		log.Errorf("failed to read transactions directory: %v", err)
		return err
	}

	for _, file := range files {
		go fs.ReadAsBatches(filepath.Join(dataDir, file.Name()), ch, fromRecordFunc)

		for batch := range ch {
			dataBatch, err := proto.Marshal(&data_batch.DataBatch{
				TaskType: int32(taskType),
				Done:     false,
				Payload:  makeBatchFunc(batch),
			})
			if err != nil {
				continue
			}

			_ = conn.SendData(dataBatch)
		}
	}

	donePayload, err := proto.Marshal(&data_batch.DataBatch{
		TaskType: int32(taskType),
		Done:     true,
	})
	if err != nil {
		return err
	}

	if err := conn.SendData(donePayload); err != nil {
		return err
	}

	return nil
}
