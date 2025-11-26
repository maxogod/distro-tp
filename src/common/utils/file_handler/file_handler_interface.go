package file_handler

// FileHandler interface for handling file operations including reading and writing CSV files in batches.
type FileHandler interface {

	// InitReader reads the file at path and sends batches of objects of type T to the proto_ch channel.
	InitReader(path string) (chan []byte, error)

	// InitReadUpTo reads up to amount lines from the file at path
	InitReadUpTo(path string, amount int) (chan []byte, error)

	// GetFileSize returns the number of lines in the file at path.
	// If the file is being written to while this method is called,
	// the retuned value is a snapshot of the number of lines at the time of the call.
	GetFileSize(path string) (int, error)

	// InitWriter writes data to the file at path from the byte_ch channel.
	InitWriter(path string) (*FileWriter, error)

	// Close releases any resources held by the FileHandler
	Close()

	// Delete the file
	DeleteFile(path string)
}

type FileWriter struct {
	storeCh chan []byte
	syncCh  chan bool
}

func (fw *FileWriter) Write(data []byte) {
	fw.storeCh <- data
}

func (fw *FileWriter) FinishWriting() {
	fw.storeCh <- []byte{FINISH_DATA_BYTE}
	<-fw.syncCh
}

func (fw *FileWriter) Sync() {
	fw.storeCh <- []byte{SYNC_DATA_BYTE}
	<-fw.syncCh
}
