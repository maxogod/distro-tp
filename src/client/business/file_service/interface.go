package file_service

import "google.golang.org/protobuf/proto"

// FileReader reads the given csv batch by batch and produces to the channel
type FileService interface {

	// ReadAsBatches reads the CSV file at csvPath and sends batches of objects of type T to the batches_ch channel.
	ReadAsBatches(csvPath string, batches_ch chan []proto.Message, newObject func(record []string) proto.Message)

	// SaveCsvAsBatches saves the CSV file at csvPath in batches to the batches_ch channel, including the provided header.
	SaveCsvAsBatches(csvPath string, batches_ch chan string, header string, done chan bool) error

	// Close releases any resources held by the FileService.
	Close()
}
