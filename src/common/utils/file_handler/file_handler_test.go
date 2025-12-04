package file_handler

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var Transactions = []*raw.Transaction{
	{
		TransactionId: "1",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 06:01:00",
	},
	{
		TransactionId: "1",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 06:00:00",
	},
	{
		TransactionId: "2",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 23:00:00",
	},
}

func TestSaveAndReadData(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	fh := NewFileHandler()
	path := "test-1"
	defer func() {
		fh.Close()
		fh.DeleteFile(path)
	}()

	writer, err := fh.InitWriter(path)
	if err != nil {
		t.Errorf("Cannot init writer: %v", err)
	}

	for _, data := range Transactions {
		bytes, _ := proto.Marshal(data)
		writer.Write(bytes)
	}
	writer.FinishWriting()

	readCh, err := fh.InitReader(path)
	if err != nil {
		t.Errorf("Cannot init reader: %v", err)
	}

	var total []*raw.Transaction
	for msg := range readCh {
		var tr raw.Transaction
		if err := proto.Unmarshal(msg, &tr); err != nil {
			t.Errorf("proto unmarshal error: %v", err)
			continue
		}
		total = append(total, &tr)
	}

	assert.Len(t, total, len(Transactions), "Number of transactions read back mismatch")

	for i, tr := range Transactions {
		if total[i].TransactionId != tr.TransactionId || total[i].FinalAmount != tr.FinalAmount {
			t.Errorf("expected transaction %v, got %v", tr, total[i])
		}
	}
}

func TestReadDataInDifferentRoutines(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	fh := NewFileHandler()
	path := "test-1"
	defer func() {
		fh.Close()
		fh.DeleteFile(path)
	}()

	writer, err := fh.InitWriter(path)
	if err != nil {
		t.Errorf("Cannot init writer: %v", err)
	}

	for _, data := range Transactions {
		bytes, _ := proto.Marshal(data)
		writer.Write(bytes)
	}
	writer.FinishWriting()

	totalReadCh := make(chan int)
	amountOfReaders := 3

	for range amountOfReaders {
		newReadCh, err := fh.InitReader(path)
		if err != nil {
			t.Errorf("Cannot init reader: %v", err)
		}

		go func() {
			totalRead := 0
			for msg := range newReadCh {
				var tr raw.Transaction
				if err := proto.Unmarshal(msg, &tr); err != nil {
					t.Errorf("proto unmarshal error: %v", err)
					continue
				}
				totalRead++
			}

			totalReadCh <- totalRead
		}()
	}
	for total := range totalReadCh {
		assert.Equal(t, len(Transactions), total, "Number of transactions read back mismatch")
		amountOfReaders--
		if amountOfReaders == 0 {
			close(totalReadCh)
		}
	}
}

func TestGetFileSize(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	fh := NewFileHandler()
	path := "test-1"
	defer func() {
		fh.Close()
		fh.DeleteFile(path)
	}()

	writer, err := fh.InitWriter(path)
	if err != nil {
		t.Errorf("Cannot init writer: %v", err)
	}

	for _, data := range Transactions {
		bytes, _ := proto.Marshal(data)
		writer.Write(bytes)
	}
	writer.FinishWriting()

	size, err := fh.GetFileSize(path)
	if err != nil {
		t.Errorf("Cannot get file size: %v", err)
	}

	assert.Equal(t, len(Transactions), size, "File size mismatch")
}

func TestGetFileSizeScreenshot(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	fh := NewFileHandler()
	path := "test-1"
	defer func() {
		fh.Close()
		fh.DeleteFile(path)
	}()

	writer, err := fh.InitWriter(path)
	if err != nil {
		t.Errorf("Cannot init writer: %v", err)
	}

	for i := range 100 {
		data := Transactions[i%len(Transactions)]
		bytes, _ := proto.Marshal(data)
		writer.Write(bytes)
	}

	size, err := fh.GetFileSize(path)
	if err != nil {
		t.Errorf("Cannot get file size: %v", err)
	}
	writer.FinishWriting()

	readCh, err := fh.InitReader(path)
	if err != nil {
		t.Errorf("Cannot init reader: %v", err)
	}

	var total []*raw.Transaction
	for msg := range readCh {
		var tr raw.Transaction
		if err := proto.Unmarshal(msg, &tr); err != nil {
			t.Errorf("proto unmarshal error: %v", err)
			continue
		}
		total = append(total, &tr)
	}
	t.Logf("File size: %d | Get size read: %d", size, len(total))
	assert.Greater(t, len(total), size, "File size mismatch")
}

func TestSyncInmediatelyAfterWrite(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	fh := NewFileHandler()
	path := "test-1"
	defer func() {
		fh.Close()
		fh.DeleteFile(path)
	}()

	writer, err := fh.InitWriter(path)
	if err != nil {
		t.Errorf("Cannot init writer: %v", err)
	}

	bytes := []byte{0x0a}
	filesize := atomic.Int64{}
	amountToSync := 10

	syncTest := make(chan bool)

	go func() {
		for i := range 10000 {
			writer.Write(bytes)

			if i == amountToSync-1 {
				writer.Sync()
				fz, _ := fh.GetFileSize(path)
				filesize.Store(int64(fz))
			}
		}
		writer.FinishWriting()
		syncTest <- true
	}()
	// we wait until the filesize is updated
	for {
		if filesize.Load() > 0 {
			break
		}
	}
	assert.Equal(t, int64(amountToSync), filesize.Load(), "File size after sync mismatch")
	<-syncTest
	close(syncTest)
}

func TestAssertNoErrorWhenClosing(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	fh := NewFileHandler()
	path := "test"
	defer func() {
		fh.DeleteFile(path)
		fh.Close()
	}()

	bytes := []byte{0x0a}

	// We initiate multiple writers to the same path
	for i := range 5 {
		writer, err := fh.InitWriter(fmt.Sprintf("%s-%d", path, i))
		if err != nil {
			t.Errorf("Cannot init writer: %v", err)
		}
		go func() {
			for range 10000 {
				writer.Write(bytes)
				writer.Sync()
			}
		}()
	}
	time.Sleep(2 * time.Second)
	fh.Close()
	for i := range 5 {
		fh.DeleteFile(fmt.Sprintf("%s-%d", path, i))
	}
}

func TestDeleteFileWhileWriting(t *testing.T) {
	logger.InitLogger(logger.LoggerEnvDevelopment)
	fh := NewFileHandler()
	path := "test"

	bytes := []byte{0x0a}

	writer, err := fh.InitWriter(path)
	if err != nil {
		t.Errorf("Cannot init writer: %v", err)
	}
	go func() {
		for range 10000 {
			writer.Write(bytes)
			writer.Sync()
		}
	}()
	time.Sleep(2 * time.Second)
	fh.DeleteFile(path)
	fh.Close()
}
