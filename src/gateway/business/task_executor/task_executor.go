package task_executor

import (
	"os"
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/business/file_service"
	"github.com/maxogod/distro-tp/src/gateway/internal/utils"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type taskExecutor struct {
	dataPath   string
	outputPath string
	conn       *network.ConnectionInterface
}

func NewTaskExecutor(dataPath, outputPath string, conn *network.ConnectionInterface) TaskExecutor {
	return &taskExecutor{
		dataPath:   dataPath,
		outputPath: outputPath,
		conn:       conn,
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

	// TODO modularize saving to file
	if err := os.MkdirAll(t.outputPath, 0755); err != nil {
		log.Errorf("failed to create output directory: %v", err)
		return err
	}

	outputFile, err := os.OpenFile(t.outputPath+"/t1.csv", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("failed to create output file: %v", err)
		return err
	}
	defer outputFile.Close()

	outputFile.WriteString("transaction_id,store_id,payment_method,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at\n")
	for {
		res, err := t.conn.ReceiveData()
		if err != nil {
			log.Errorf("failed to receive response from server: %v", err)
			return err
		}
		dataBatch := &data_batch.DataBatch{}
		if err := proto.Unmarshal(res, dataBatch); err != nil {
			log.Errorf("failed to unmarshal response from server: %v", err)
			return err
		} else if dataBatch.Done {
			break // No more batches
		}

		transactionBatch := &raw.TransactionBatch{}
		if err := proto.Unmarshal(dataBatch.Payload, transactionBatch); err != nil {
			log.Errorf("failed to unmarshal transaction batch from server: %v", err)
			return err
		}

		for _, transaction := range transactionBatch.Transactions {
			line := utils.TransactionToCsv(transaction)
			if _, err := outputFile.WriteString(line); err != nil {
				log.Errorf("failed to write to output file: %v", err)
				return err
			}
		}
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

	// TODO save to file

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

	// TODO save to file

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

	// TODO save to file

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

	files, err := os.ReadDir(dataDir)
	if err != nil {
		log.Errorf("failed to read transactions directory: %v", err)
		return err
	}

	for _, file := range files {
		ch := make(chan []T)
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
