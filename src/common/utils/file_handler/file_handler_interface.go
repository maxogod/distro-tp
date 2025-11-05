package file_handler

import "google.golang.org/protobuf/proto"

// FileHandler interface for handling file operations including reading and writing CSV files in batches.
type FileHandler interface {

	// ReadAsBatches reads the file at path and sends batches of objects of type T to the proto_ch channel.
	ReadAsBatches(path string, batch_ch chan [][]byte) error

	// SaveAsBatches saves the file at path in batches to the proto_ch channel.
	SaveProtoData(path string, proto_ch chan proto.Message) error

	// SaveIndexedData performs a search and update operation in the file at path.
	SaveIndexedData(path string, dataKey string, updateFunc func(*[]byte)) error

	// Close releases any resources held by the FileHandler
	Close()

	// CloseFile closes the file at path.
	CloseFile(path string) error

	// Delete the file
	DeleteFile(path string) error
}
