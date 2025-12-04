package file_service

import (
	"encoding/csv"
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
	"google.golang.org/protobuf/proto"
)

type fileService struct {
	batchSize int
	openFiles []*os.File
}

func NewFileService(batchSize int) FileService {
	return &fileService{
		batchSize: batchSize,
		openFiles: make([]*os.File, 0),
	}
}

func (fs *fileService) ReadAsBatches(path string, batchesCh chan []proto.Message, newObject func([]string) proto.Message) {
	logger.Logger.Debugln("Reading from file:", path)

	defer close(batchesCh)
	file, err := os.Open(path)
	if err != nil {
		return
	}
	fs.openFiles = append(fs.openFiles, file)
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return
	}

	batch := make([]proto.Message, 0, fs.batchSize)
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		t := newObject(record)
		batch = append(batch, t)

		if len(batch) >= fs.batchSize {
			batchesCh <- batch
			batch = make([]proto.Message, 0, fs.batchSize)
		}
	}

	if len(batch) > 0 {
		batchesCh <- batch
	}
}

func (fs *fileService) SaveCsvAsBatches(path string, batchesCh chan string, header string, done chan bool) error {
	logger.Logger.Debugln("Creating file:", path)

	outputFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		logger.Logger.Errorf("failed to create output file: %v", err)
		return err
	}
	fs.openFiles = append(fs.openFiles, outputFile)
	defer outputFile.Close()

	logger.Logger.Debugln("Writing to file:", path)

	outputFile.WriteString(header)
	for entry := range batchesCh {
		if _, err := outputFile.WriteString(entry); err != nil {
			break
		}
	}

	logger.Logger.Debugln("Finished writing to file:", path)

	done <- true

	return nil
}

func (fs *fileService) Close() {
	for _, file := range fs.openFiles {
		file.Close() // Double close is safe
	}
}
