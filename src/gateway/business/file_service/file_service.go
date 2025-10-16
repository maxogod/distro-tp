package file_service

import (
	"encoding/csv"
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

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

func (fs *fileService) ReadAsBatches(path string, batches_ch chan []proto.Message, newObject func([]string) proto.Message) {
	log.Debugln("Reading from file:", path)

	defer close(batches_ch)
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
			batches_ch <- batch
			batch = make([]proto.Message, 0, fs.batchSize)
		}
	}

	if len(batch) > 0 {
		batches_ch <- batch
	}
}

func (fs *fileService) SaveCsvAsBatches(path string, batches_ch chan string, header string) error {
	outputFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("failed to create output file: %v", err)
		return err
	}
	fs.openFiles = append(fs.openFiles, outputFile)
	defer outputFile.Close()

	outputFile.WriteString(header)
	for entry := range batches_ch {
		if _, err := outputFile.WriteString(entry); err != nil {
			break
		}
	}

	return nil
}

func (fs *fileService) Close() {
	for _, file := range fs.openFiles {
		file.Close() // Double close is safe
	}
}
