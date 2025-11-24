package file_handler

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
)

const FLUSH_DATA_BYTE = 0xff
const FLUSH_THRESHOLD = 64 * 1024 // aprox 64KB

type fileStoreHandler struct {
	finishCh chan bool
	file     *os.File
	storeCh  chan []byte
}

type fileHandler struct {
	openFiles map[string]fileStoreHandler
}

func NewFileHandler() FileHandler {
	return &fileHandler{
		openFiles: make(map[string]fileStoreHandler),
	}
}

// Reads protobuf bytes in batches from file.
func (fh *fileHandler) ReadData(
	path string,
	proto_ch chan []byte,
) error {

	var file *os.File
	openFile, ok := fh.openFiles[path]

	if ok {
		// logger.Logger.Debug("file already present!")
		fh.finishWritingFile(path)
		file = openFile.file
		if _, err := file.Seek(0, 0); err != nil {
			logger.Logger.Errorf("failed to seek to beginning of file: %v", err)
			return err
		}
	} else {

		newOpenFile, err := os.Open(path)
		if err != nil {
			logger.Logger.Errorf("failed to open file: %v", err)
			return err
		}
		fh.openFiles[path] = fileStoreHandler{
			file:     newOpenFile,
			finishCh: nil,
			storeCh:  nil,
		}
		file = newOpenFile
	}

	scanner := bufio.NewScanner(file)

	go func() {
		//defer file.Close()
		defer close(proto_ch)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			protoBytes, err := parseFromBytes(line)
			if err != nil {
				logger.Logger.Errorf("failed to parse from bytes: %v", err)
			}
			proto_ch <- protoBytes
		}

		if err := scanner.Err(); err != nil {
			logger.Logger.Errorf("scanner error: %v", err)
		}
	}()

	return nil
}

func (fh *fileHandler) WriteData(path string, byte_ch chan []byte) error {

	outputFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)

	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	finishCh := make(chan bool)
	fh.openFiles[path] = fileStoreHandler{
		finishCh: finishCh,
		storeCh:  byte_ch,
		file:     outputFile,
	}

	defer func() {
		close(finishCh)
		close(byte_ch)
		fh.openFiles[path] = fileStoreHandler{
			finishCh: nil,
			storeCh:  nil,
			file:     outputFile,
		}
	}()

	writer := bufio.NewWriter(outputFile)

	for entry := range byte_ch {
		if bytes.Equal(entry, []byte{FLUSH_DATA_BYTE}) {
			break
		}

		if len(entry) == 0 {
			continue
		}

		line := parseToString(entry)
		if _, err := writer.WriteString(line); err != nil {
			return err
		}

		// Flush periodically for safety
		if writer.Buffered() > FLUSH_THRESHOLD {
			if err := writer.Flush(); err != nil {
				return err
			}
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}
	if err := outputFile.Sync(); err != nil {
		return err
	}
	finishCh <- true

	return nil
}

func (fh *fileHandler) Close() {
	for path := range fh.openFiles {
		fh.finishWritingFile(path)
	}
}

func (fh *fileHandler) DeleteFile(path string) {
	fh.finishWritingFile(path)
	storeHandler, ok := fh.openFiles[path]
	if ok {
		storeHandler.file.Close()
	}
	delete(fh.openFiles, path)
	os.Remove(path)
}

// ========== Private Methods ===========

func (fh *fileHandler) finishWritingFile(path string) {

	storeHandler, ok := fh.openFiles[path]
	if !ok {
		logger.Logger.Warnf("No open file found for path: %s", path)
		return
	}
	if storeHandler.finishCh != nil && storeHandler.storeCh != nil {
		storeHandler.storeCh <- []byte{FLUSH_DATA_BYTE}
		<-storeHandler.finishCh
	}
}

func parseToString(bytes []byte) string {
	encodedData := base64.StdEncoding.EncodeToString(bytes)
	return encodedData + "\n"
}

func parseFromBytes(line []byte) ([]byte, error) {
	protoBytes, err := base64.StdEncoding.DecodeString(string(line))
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	return protoBytes, nil
}
