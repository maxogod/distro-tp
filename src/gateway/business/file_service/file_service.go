package file_service

import (
	"encoding/csv"
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
)

var log = logger.GetLogger()

type fileService[T any] struct {
	batchSize int
}

func NewFileService[T any](batchSize int) FileService[T] {
	return &fileService[T]{
		batchSize: batchSize,
	}
}

func (fs *fileService[T]) ReadAsBatches(path string, batches_ch chan []T, newObject func([]string) T) {
	log.Debugln("Reading from file:", path)

	defer close(batches_ch)
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return
	}

	batch := make([]T, 0, fs.batchSize)
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		t := newObject(record)
		batch = append(batch, t)

		if len(batch) >= fs.batchSize {
			batches_ch <- batch
			batch = make([]T, 0, fs.batchSize)
		}
	}

	if len(batch) > 0 {
		batches_ch <- batch
	}
}

func (fs *fileService[T]) SaveCsvAsBatches(path string, batches_ch chan string, header string) error {
	outputFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("failed to create output file: %v", err)
		return err
	}
	defer outputFile.Close()

	outputFile.WriteString(header)
	for entry := range batches_ch {
		if _, err := outputFile.WriteString(entry); err != nil {
			log.Errorf("failed to write to output file: %v", err)
			return err
		}
	}

	return nil
}
