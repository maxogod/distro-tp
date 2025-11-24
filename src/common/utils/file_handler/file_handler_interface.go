package file_handler

// FileHandler interface for handling file operations including reading and writing CSV files in batches.
type FileHandler interface {

	// ReadAll reads the file at path and sends batches of objects of type T to the proto_ch channel.
	ReadData(path string, proto_ch chan []byte) error

	// WriteData writes data to the file at path from the byte_ch channel.
	WriteData(path string, byte_ch chan []byte) error

	// Close releases any resources held by the FileHandler
	Close()

	// Delete the file
	DeleteFile(path string)
}
