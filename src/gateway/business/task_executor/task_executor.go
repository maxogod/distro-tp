package task_executor

import (
	"os"
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/business/file_service"
	"github.com/maxogod/distro-tp/src/gateway/internal/utils"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

type taskExecutor struct {
	dataPath   string
	outputPath string
	batchSize  int
	conn       network.ConnectionInterface
}

func NewTaskExecutor(dataPath, outputPath string, batchSize int, conn network.ConnectionInterface) TaskExecutor {
	return &taskExecutor{
		dataPath:   dataPath,
		outputPath: outputPath,
		batchSize:  batchSize,
		conn:       conn,
	}
}

func (t *taskExecutor) Task1() error {
	transactionsDir := t.dataPath + TransactionsDirPath
	err := readAndSendData(
		t.conn,
		enum.T1,
		transactionsDir,
		t.batchSize,
		false,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	log.Debug("All transactions data sent, waiting for results...")

	receiveAndSaveResults(
		t.conn,
		filepath.Join(t.outputPath, OUTPUT_FILE_T1),
		utils.T1_RES_HEADER,
		t.batchSize,
		func(dataBatch *protocol.DataEnvelope, ch chan string) {
			transactionBatch := &raw.TransactionBatch{}
			if err := proto.Unmarshal(dataBatch.Payload, transactionBatch); err != nil {
				log.Errorf("failed to unmarshal transaction batch from server: %v", err)
				return
			}

			for _, transaction := range transactionBatch.Transactions {
				line := utils.TransactionToCsv(transaction)
				ch <- line
			}
		},
	)

	return nil
}

func (t *taskExecutor) Task2() error {
	menuItemsDir := t.dataPath + MenuItemsDirPath
	err := readAndSendData(
		t.conn,
		enum.T2,
		menuItemsDir,
		t.batchSize,
		true,
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
		t.batchSize,
		false,
		utils.TransactionItemsFromRecord,
		utils.TransactionItemsBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transaction items data: %v", err)
		return err
	}

	receiveAndSaveResults(
		t.conn,
		filepath.Join(t.outputPath, OUTPUT_FILE_T2_1),
		utils.T2_1_RES_HEADER,
		t.batchSize,
		func(dataBatch *protocol.DataEnvelope, ch chan string) {
			data := &reduced.TotalProfitBySubtotal{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				log.Errorf("failed to unmarshal transaction items batch from server: %v", err)
				return
			}

			line := utils.MostProfitableItemsToCsv(data)
			log.Debugf("Received line: %s", line)
			ch <- line
		},
	)

	receiveAndSaveResults(
		t.conn,
		filepath.Join(t.outputPath, OUTPUT_FILE_T2_2),
		utils.T2_2_RES_HEADER,
		t.batchSize,
		func(dataBatch *protocol.DataEnvelope, ch chan string) {
			data := &reduced.TotalSoldByQuantity{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				log.Errorf("failed to unmarshal transaction items batch from server: %v", err)
				return
			}

			line := utils.BestSellingItemsToCsv(data)
			ch <- line
		},
	)

	return nil
}

func (t *taskExecutor) Task3() error {
	storesDir := t.dataPath + StoresDirPath
	err := readAndSendData(
		t.conn,
		enum.T3,
		storesDir,
		t.batchSize,
		true,
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
		t.batchSize,
		false,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	receiveAndSaveResults(
		t.conn,
		filepath.Join(t.outputPath, OUTPUT_FILE_T3),
		utils.T3_RES_HEADER,
		t.batchSize,
		func(dataBatch *protocol.DataEnvelope, ch chan string) {
			data := &reduced.TotalPaymentValue{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				log.Errorf("failed to unmarshal store tpv batch from server: %v", err)
				return
			}

			line := utils.TopStoresByTPVToCsv(data)
			ch <- line
		},
	)

	return nil
}

func (t *taskExecutor) Task4() error {
	usersDir := t.dataPath + UsersDirPath
	err := readAndSendData(
		t.conn,
		enum.T4,
		usersDir,
		t.batchSize,
		true,
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
		t.batchSize,
		true,
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
		t.batchSize,
		false,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	receiveAndSaveResults(
		t.conn,
		filepath.Join(t.outputPath, OUTPUT_FILE_T4),
		utils.T4_RES_HEADER,
		t.batchSize,
		func(dataBatch *protocol.DataEnvelope, ch chan string) {
			data := &reduced.CountedUserTransactions{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				log.Errorf("failed to unmarshal most purchases user batch from server: %v", err)
				return
			}

			line := utils.TopUsersByPurchasesToCsv(data)
			ch <- line
		},
	)

	return nil
}

/* --- UTILS --- */

func readAndSendData[T any](
	conn network.ConnectionInterface,
	taskType enum.TaskType,
	dataDir string,
	batchSize int,
	isRef bool,
	fromRecordFunc func([]string) T,
	makeBatchFunc func([]T) []byte,
) error {
	fs := file_service.NewFileService[T](batchSize)

	files, err := os.ReadDir(dataDir)
	if err != nil {
		log.Errorf("failed to read transactions directory: %v", err)
		return err
	}

	for _, file := range files {
		ch := make(chan []T)
		go fs.ReadAsBatches(filepath.Join(dataDir, file.Name()), ch, fromRecordFunc)

		for batch := range ch {
			dataBatch, err := proto.Marshal(&protocol.DataEnvelope{
				TaskType: int32(taskType),
				Payload:  makeBatchFunc(batch),
				IsDone:   false,
				IsRef:    isRef,
			})
			if err != nil {
				continue
			}

			_ = conn.SendData(dataBatch)
		}
	}

	donePayload, err := proto.Marshal(&protocol.DataEnvelope{
		TaskType: int32(taskType),
		IsDone:   true,
		IsRef:    isRef,
	})
	if err != nil {
		return err
	}

	if err := conn.SendData(donePayload); err != nil {
		return err
	}

	return nil
}

func receiveAndSaveResults(
	conn network.ConnectionInterface,
	path,
	header string,
	batchSize int,
	generateStringObject func(*protocol.DataEnvelope, chan string),
) error {
	fs := file_service.NewFileService[string](batchSize)

	batchesCh := make(chan string)

	go func() {
		for {
			res, err := conn.ReceiveData()
			if err != nil {
				log.Errorf("failed to receive response from server: %v", err)
				return
			}
			dataBatch := &protocol.DataEnvelope{}
			if err := proto.Unmarshal(res, dataBatch); err != nil {
				log.Errorf("failed to unmarshal response from server: %v", err)
				return
			} else if dataBatch.GetIsDone() {
				close(batchesCh)
				break // No more batches
			}

			generateStringObject(dataBatch, batchesCh)
		}
	}()

	fs.SaveCsvAsBatches(path, batchesCh, header)
	log.Debug("Finished saving data")

	return nil
}
