package task_executor

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/maxogod/distro-tp/src/client/business/file_service"
	"github.com/maxogod/distro-tp/src/client/config"
	"github.com/maxogod/distro-tp/src/client/internal/utils"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/common/network"
	"google.golang.org/protobuf/proto"
)

type taskExecutor struct {
	dataPath   string
	outputPath string
	batchSize  int
	conn       network.ConnectionInterface
	fs         file_service.FileService
	conf       *config.Config
}

func NewTaskExecutor(dataPath, outputPath string, batchSize int, conn network.ConnectionInterface, conf *config.Config) TaskExecutor {
	return &taskExecutor{
		dataPath:   dataPath,
		outputPath: outputPath,
		batchSize:  batchSize,
		conn:       conn,
		fs:         file_service.NewFileService(batchSize),
		conf:       conf,
	}
}

func (t *taskExecutor) Task1() error {
	transactionsDir := t.dataPath + t.conf.Paths.Transactions
	err := t.readAndSendData(
		enum.T1,
		transactionsDir,
		false,
		true,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		return err
	}

	logger.Logger.Debug("All transactions data sent, waiting for results...")

	t.receiveAndSaveResults(
		filepath.Join(t.outputPath, t.conf.OutputFiles.T1),
		t.conf.Headers.T1,
		func(dataBatch *protocol.DataEnvelope, ch chan string) {
			transactionBatch := &raw.TransactionBatch{}
			if err := proto.Unmarshal(dataBatch.Payload, transactionBatch); err != nil {
				logger.Logger.Errorf("failed to unmarshal transaction batch from server: %v", err)
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
	menuItemsDir := t.dataPath + t.conf.Paths.MenuItems
	err := t.readAndSendData(
		enum.T2,
		menuItemsDir,
		true,
		true,
		utils.MenuItemFromRecord,
		utils.MenuItemBatchFromList,
	)
	if err != nil {
		return err
	}

	transactionsItemsDir := t.dataPath + t.conf.Paths.TransactionItems
	err = t.readAndSendData(
		enum.T2,
		transactionsItemsDir,
		false,
		true,
		utils.TransactionItemsFromRecord,
		utils.TransactionItemsBatchFromList,
	)
	if err != nil {
		return err
	}

	// Receive and save results

	res, err := t.conn.ReceiveData()
	if err != nil {
		return err
	}
	dataEnvelope := &protocol.DataEnvelope{}
	if err := proto.Unmarshal(res, dataEnvelope); err != nil {
		logger.Logger.Errorf("failed to unmarshal response from server: %v", err)
		return nil
	}
	data := &reduced.TotalSumItemsReport{}
	if err := proto.Unmarshal(dataEnvelope.Payload, data); err != nil {
		logger.Logger.Errorf("failed to unmarshal counted user transactions batch from server: %v", err)
		return nil
	}

	t.saveEntireResults(
		filepath.Join(t.outputPath, t.conf.OutputFiles.T2_1),
		t.conf.Headers.T2_1,
		func(ch chan string) {
			for _, item := range data.GetTotalSumItemsBySubtotal() {
				line := utils.MostProfitableItemsToCsv(item)
				ch <- line
			}
		},
	)

	t.saveEntireResults(
		filepath.Join(t.outputPath, t.conf.OutputFiles.T2_2),
		t.conf.Headers.T2_2,
		func(ch chan string) {
			for _, item := range data.GetTotalSumItemsByQuantity() {
				line := utils.BestSellingItemsToCsv(item)
				ch <- line
			}
		},
	)

	return nil
}

func (t *taskExecutor) Task3() error {
	storesDir := t.dataPath + t.conf.Paths.Stores
	err := t.readAndSendData(
		enum.T3,
		storesDir,
		true,
		true,
		utils.StoreFromRecord,
		utils.StoreBatchFromList,
	)
	if err != nil {
		return err
	}

	transactionsDir := t.dataPath + t.conf.Paths.Transactions
	err = t.readAndSendData(
		enum.T3,
		transactionsDir,
		false,
		true,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		return err
	}

	err = t.receiveAndSaveEntireResults(
		filepath.Join(t.outputPath, t.conf.OutputFiles.T3),
		t.conf.Headers.T3,
		func(dataBatch *protocol.DataEnvelope, ch chan string) {
			data := &reduced.TotalPaymentValueBatch{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				logger.Logger.Errorf("failed to unmarshal counted user transactions batch from server: %v", err)
				return
			}
			for _, countedUserTransaction := range data.GetTotalPaymentValues() {
				line := utils.TopStoresByTPVToCsv(countedUserTransaction)
				ch <- line
			}
		},
	)
	if err != nil {
		return err
	}
	return nil
}

func (t *taskExecutor) Task4() error {
	usersDir := t.dataPath + t.conf.Paths.Users
	err := t.readAndSendData(
		enum.T4,
		usersDir,
		true,
		false, // Only send done once after all ref data is sent
		utils.UserFromRecord,
		utils.UserBatchFromList,
	)
	if err != nil {
		return err
	}

	storesDir := t.dataPath + t.conf.Paths.Stores
	err = t.readAndSendData(
		enum.T4,
		storesDir,
		true,
		true,
		utils.StoreFromRecord,
		utils.StoreBatchFromList,
	)
	if err != nil {
		return err
	}

	transactionsDir := t.dataPath + t.conf.Paths.Transactions
	err = t.readAndSendData(
		enum.T4,
		transactionsDir,
		false,
		true,
		utils.TransactionFromRecord,
		utils.TransactionBatchFromList,
	)
	if err != nil {
		return err
	}

	// Receive and save results

	err = t.receiveAndSaveEntireResults(
		filepath.Join(t.outputPath, t.conf.OutputFiles.T4),
		t.conf.Headers.T4,
		func(dataBatch *protocol.DataEnvelope, ch chan string) {
			data := &reduced.CountedUserTransactionBatch{}
			if err := proto.Unmarshal(dataBatch.Payload, data); err != nil {
				logger.Logger.Errorf("failed to unmarshal counted user transactions batch from server: %v", err)
				return
			}
			for _, countedUserTransaction := range data.CountedUserTransactions {
				line := utils.TopUsersByPurchasesToCsv(countedUserTransaction)
				ch <- line
			}
		},
	)
	if err != nil {
		return err
	}
	return nil
}

/* --- UTILS --- */

func (t *taskExecutor) readAndSendData(
	taskType enum.TaskType,
	dataDir string,
	isRef bool,
	sendDone bool,
	fromRecordFunc func([]string) proto.Message,
	makeBatchFunc func([]proto.Message) []byte,
) error {
	files, err := os.ReadDir(dataDir)
	if err != nil {
		logger.Logger.Errorf("failed to read transactions directory: %v", err)
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

func (t *taskExecutor) receiveAndSaveResults(
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
				logger.Logger.Debugf("connection with server closed")
				return
			}
			dataBatch := &protocol.DataEnvelope{}
			if err := proto.Unmarshal(res, dataBatch); err != nil {
				logger.Logger.Errorf("failed to unmarshal response from server: %v", err)
				return
			} else if dataBatch.GetIsDone() {
				break // No more batches
			}

			generateStringObject(dataBatch, batchesCh)
		}
	}()

	t.fs.SaveCsvAsBatches(path, batchesCh, header)
	logger.Logger.Debug("Finished saving data")

	return nil
}

func (t *taskExecutor) receiveAndSaveEntireResults(
	path,
	header string,
	generateStringObject func(*protocol.DataEnvelope, chan string),
) error {
	batchesCh := make(chan string)

	res, err := t.conn.ReceiveData()
	if err != nil {
		logger.Logger.Debugf("connection with server closed %v", err)
		return err
	}
	dataEnvelope := &protocol.DataEnvelope{}
	if err = proto.Unmarshal(res, dataEnvelope); err != nil {
		logger.Logger.Errorf("failed to unmarshal response from server: %v", err)
		return err
	}

	go func() {
		defer close(batchesCh)
		generateStringObject(dataEnvelope, batchesCh)
	}()

	t.fs.SaveCsvAsBatches(path, batchesCh, header)
	logger.Logger.Debug("Finished saving data")

	return nil
}

func (t *taskExecutor) saveEntireResults(
	path,
	header string,
	generateStringObject func(chan string),
) error {
	batchesCh := make(chan string)
	go func() {
		defer close(batchesCh)
		generateStringObject(batchesCh)
	}()
	t.fs.SaveCsvAsBatches(path, batchesCh, header)
	logger.Logger.Debug("Finished saving data")
	return nil
}

func (t *taskExecutor) Close() {
	t.fs.Close()
}

func (t *taskExecutor) SendRequestForTask(taskType enum.TaskType, clientId string) error {
	if clientId == "" {
		clientId = uuid.Nil.String()
	}

	msg := &protocol.ControlMessage{
		TaskType: int32(taskType),
		ClientId: clientId,
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	return t.conn.SendData(payload)
}

func (t *taskExecutor) AwaitRequestAck(taskType enum.TaskType) (string, error) {
	data, err := t.conn.ReceiveData()
	if err != nil {
		return "", err
	}

	controlMsg := &protocol.ControlMessage{}
	if err = proto.Unmarshal(data, controlMsg); err != nil {
		return "", err
	}

	if !controlMsg.GetIsAck() {
		return "", fmt.Errorf("received non-ack control message for task %d", taskType)
	}

	if controlMsg.GetTaskType() != int32(taskType) {
		return "", fmt.Errorf("received ack for unexpected task type %d", controlMsg.GetTaskType())
	}

	clientId := controlMsg.GetClientId()
	logger.Logger.Debugf("Received ack from gateway for task %d with client ID %s", taskType, clientId)

	return clientId, nil
}
