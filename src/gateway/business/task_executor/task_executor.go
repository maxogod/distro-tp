package task_executor

import (
	"os"
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/joined"
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
	batchSize  int
	conn       *network.ConnectionInterface
}

func NewTaskExecutor(dataPath, outputPath string, batchSize int, conn *network.ConnectionInterface) TaskExecutor {
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
		enum.NoRef,
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
		filepath.Join(t.outputPath, "t1.csv"),
		utils.T1_RES_HEADER,
		t.batchSize,
		func(dataBatch *data_batch.DataBatch, ch chan string) {
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

	log.Debug("Results saved successfully")

	return nil
}

func (t *taskExecutor) Task2() error {
	menuItemsDir := t.dataPath + MenuItemsDirPath
	err := readAndSendData(
		t.conn,
		enum.T2,
		menuItemsDir,
		t.batchSize,
		enum.MenuItems,
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
		enum.NoRef,
		utils.TransactionItemsFromRecord,
		utils.TransactionItemsBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transaction items data: %v", err)
		return err
	}

	receiveAndSaveResults(
		t.conn,
		filepath.Join(t.outputPath, "t2_1.csv"),
		utils.T2_1_RES_HEADER,
		t.batchSize,
		func(dataBatch *data_batch.DataBatch, ch chan string) {
			data := &joined.JoinBestSellingProductsBatch{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				log.Errorf("failed to unmarshal transaction items batch from server: %v", err)
				return
			}

			for _, transactionItems := range data.Items {
				line := utils.BestSellingItemsToCsv(transactionItems)
				ch <- line
			}
		},
	)

	receiveAndSaveResults(
		t.conn,
		filepath.Join(t.outputPath, "t2_2.csv"),
		utils.T2_1_RES_HEADER,
		t.batchSize,
		func(dataBatch *data_batch.DataBatch, ch chan string) {
			data := &joined.JoinMostProfitsProductsBatch{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				log.Errorf("failed to unmarshal transaction items batch from server: %v", err)
				return
			}

			for _, transactionItems := range data.Items {
				line := utils.MostProfitableItemsToCsv(transactionItems)
				ch <- line
			}
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
		enum.Stores,
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
		enum.NoRef,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	receiveAndSaveResults(
		t.conn,
		filepath.Join(t.outputPath, "t3.csv"),
		utils.T3_RES_HEADER,
		t.batchSize,
		func(dataBatch *data_batch.DataBatch, ch chan string) {
			data := &joined.JoinStoreTPVBatch{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				log.Errorf("failed to unmarshal store tpv batch from server: %v", err)
				return
			}

			for _, storeTPV := range data.Items {
				line := utils.TopStoresByTPVToCsv(storeTPV)
				ch <- line
			}
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
		enum.Users,
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
		enum.Stores,
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
		enum.NoRef,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		log.Errorf("failed to send transactions data: %v", err)
		return err
	}

	receiveAndSaveResults(
		t.conn,
		filepath.Join(t.outputPath, "t4.csv"),
		utils.T4_RES_HEADER,
		t.batchSize,
		func(dataBatch *data_batch.DataBatch, ch chan string) {
			data := &joined.JoinMostPurchasesUserBatch{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				log.Errorf("failed to unmarshal most purchases user batch from server: %v", err)
				return
			}

			for _, mostPurchasesUser := range data.Users {
				line := utils.TopUsersByPurchasesToCsv(mostPurchasesUser)
				ch <- line
			}
		},
	)

	return nil
}

/* --- UTILS --- */

func readAndSendData[T any](
	conn *network.ConnectionInterface,
	taskType enum.TaskType,
	dataDir string,
	batchSize int,
	refDataType enum.RefDatasetType,
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
			dataBatch, err := proto.Marshal(&data_batch.DataBatch{
				TaskType:        int32(taskType),
				Done:            false,
				IsReferenceData: refDataType != enum.NoRef,
				RefDataType:     int32(refDataType),
				Payload:         makeBatchFunc(batch),
			})
			if err != nil {
				continue
			}

			_ = conn.SendData(dataBatch)
		}
	}

	donePayload, err := proto.Marshal(&data_batch.DataBatch{
		TaskType:        int32(taskType),
		IsReferenceData: refDataType != enum.NoRef,
		RefDataType:     int32(refDataType),
		Done:            true,
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
	conn *network.ConnectionInterface,
	path,
	header string,
	batchSize int,
	generateStringObject func(*data_batch.DataBatch, chan string),
) error {
	fs := file_service.NewFileService[string](batchSize)

	batchesCh := make(chan string)
	go fs.SaveCsvAsBatches(path, batchesCh, header)

	for {
		res, err := conn.ReceiveData()
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

		generateStringObject(dataBatch, batchesCh)
	}

	// TODO: all data and done should only be sent by aggregator
	log.Debug("Finished saving data")

	return nil
}
