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
	fs         file_service.FileService
}

func NewTaskExecutor(dataPath, outputPath string, batchSize int, conn network.ConnectionInterface) TaskExecutor {
	return &taskExecutor{
		dataPath:   dataPath,
		outputPath: outputPath,
		batchSize:  batchSize,
		conn:       conn,
		fs:         file_service.NewFileService(batchSize),
	}
}

func (t *taskExecutor) Task1() error {
	transactionsDir := t.dataPath + TransactionsDirPath
	err := t.readAndSendData(
		enum.T1,
		transactionsDir,
		false,
		true,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	log.Debug("All transactions data sent, waiting for results...")

	t.receiveAndSaveResults(
		filepath.Join(t.outputPath, OUTPUT_FILE_T1),
		utils.T1_RES_HEADER,
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
	err := t.readAndSendData(
		enum.T2,
		menuItemsDir,
		true,
		true,
		utils.MenuItemFromRecord,
		utils.MenuItemBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send menu items data: %v", err)
		return err
	}

	transactionsItemsDir := t.dataPath + TransactionItemsDirPath
	err = t.readAndSendData(
		enum.T2,
		transactionsItemsDir,
		false,
		true,
		utils.TransactionItemsFromRecord,
		utils.TransactionItemsBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transaction items data: %v", err)
		return err
	}

	t.receiveAndSaveResults(
		filepath.Join(t.outputPath, OUTPUT_FILE_T2_1),
		utils.T2_1_RES_HEADER,
		func(dataBatch *protocol.DataEnvelope, ch chan string) {
			data := &reduced.TotalProfitBySubtotal{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				log.Errorf("failed to unmarshal transaction items batch from server: %v", err)
				return
			}

			line := utils.MostProfitableItemsToCsv(data)
			ch <- line
		},
	)

	t.receiveAndSaveResults(
		filepath.Join(t.outputPath, OUTPUT_FILE_T2_2),
		utils.T2_2_RES_HEADER,
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
	err := t.readAndSendData(
		enum.T3,
		storesDir,
		true,
		true,
		utils.StoreFromRecord,
		utils.StoreBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send stores data: %v", err)
		return err
	}

	transactionsDir := t.dataPath + TransactionsDirPath
	err = t.readAndSendData(
		enum.T3,
		transactionsDir,
		false,
		true,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	t.receiveAndSaveResults(
		filepath.Join(t.outputPath, OUTPUT_FILE_T3),
		utils.T3_RES_HEADER,
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
	err := t.readAndSendData(
		enum.T4,
		usersDir,
		true,
		false, // Only send done once after all ref data is sent
		utils.UserFromRecord,
		utils.UserBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send users data: %v", err)
		return err
	}

	storesDir := t.dataPath + StoresDirPath
	err = t.readAndSendData(
		enum.T4,
		storesDir,
		true,
		true,
		utils.StoreFromRecord,
		utils.StoreBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send stores data: %v", err)
		return err
	}

	transactionsDir := t.dataPath + TransactionsDirPath
	err = t.readAndSendData(
		enum.T4,
		transactionsDir,
		false,
		true,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	t.receiveAndSaveResults(
		filepath.Join(t.outputPath, OUTPUT_FILE_T4),
		utils.T4_RES_HEADER,
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

func (t taskExecutor) readAndSendData(
	taskType enum.TaskType,
	dataDir string,
	isRef bool,
	sendDone bool,
	fromRecordFunc func([]string) proto.Message,
	makeBatchFunc func([]proto.Message) []byte,
) error {
	files, err := os.ReadDir(dataDir)
	if err != nil {
		log.Errorf("failed to read transactions directory: %v", err)
		return err
	}

	for _, file := range files {
		ch := make(chan []proto.Message)
		go t.fs.ReadAsBatches(filepath.Join(dataDir, file.Name()), ch, fromRecordFunc)

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

			if err := t.conn.SendData(dataBatch); err != nil {
				return err
			}
		}
	}

	if !sendDone {
		return nil
	}

	donePayload, err := proto.Marshal(&protocol.DataEnvelope{
		TaskType: int32(taskType),
		IsDone:   true,
		IsRef:    isRef,
	})
	if err != nil {
		return err
	}

	if err := t.conn.SendData(donePayload); err != nil {
		return err
	}

	return nil
}

func (t taskExecutor) receiveAndSaveResults(
	path,
	header string,
	generateStringObject func(*protocol.DataEnvelope, chan string),
) error {
	batchesCh := make(chan string)

	go func() {
		defer close(batchesCh)
		for {
			res, err := t.conn.ReceiveData()
			if err != nil {
				log.Debugf("connection with server closed")
				return
			}
			dataBatch := &protocol.DataEnvelope{}
			if err := proto.Unmarshal(res, dataBatch); err != nil {
				log.Errorf("failed to unmarshal response from server: %v", err)
				return
			} else if dataBatch.GetIsDone() {
				break // No more batches
			}

			generateStringObject(dataBatch, batchesCh)
		}
	}()

	t.fs.SaveCsvAsBatches(path, batchesCh, header)
	log.Debug("Finished saving data")

	return nil
}

func (t *taskExecutor) Close() {
	t.fs.Close()
}
