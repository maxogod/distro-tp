package file_handler

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
)

var log = logger.GetLogger()

const SEPERATOR = "#"
const FLUSH_DATA_BYTE = 0xff
const FLUSH_THRESHOLD = 64 * 1024 // aprox 64KB

type fileStoreHandler struct {
	finishCh chan bool
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
	log.Debugln("Reading from file:", path)

	fh.finishWritingFile(path)

	file, err := os.Open(path)
	if err != nil {
		log.Errorf("failed to open file: %v", err)
		return err
	}

	defer file.Close()
	defer close(proto_ch)

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		_, protoBytes, err := parseFromBytes(line)
		if err != nil {
			log.Errorf("failed to parse from bytes: %v", err)
			return err
		}
		proto_ch <- protoBytes
	}

	if err := scanner.Err(); err != nil {
		log.Errorf("scanner error: %v", err)
	}

	return nil
}

func (fh *fileHandler) WriteData(path string, byte_ch chan []byte) error {
	outputFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)

	log.Debugln("Writing to file:", path)

	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	finishCh := make(chan bool)
	fh.openFiles[path] = fileStoreHandler{
		finishCh: finishCh,
		storeCh:  byte_ch,
	}

	defer func() {
		close(finishCh)
		outputFile.Close()
		delete(fh.openFiles, path)
	}()

	writer := bufio.NewWriter(outputFile)

	for entry := range byte_ch {
		if bytes.Equal(entry, []byte{FLUSH_DATA_BYTE}) {
			break
		}

		if len(entry) == 0 {
			continue
		}

		line := parseToString("", entry)
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
	os.Remove(path)
	delete(fh.openFiles, path)
}

// ========== Private Methods ===========

func (fh *fileHandler) finishWritingFile(path string) {

	storeHandler, ok := fh.openFiles[path]
	if !ok {
		log.Warnf("No open file found for path: %s", path)
		return
	}
	storeHandler.storeCh <- []byte{FLUSH_DATA_BYTE}
	<-storeHandler.finishCh
}

func parseToString(dataKey string, bytes []byte) string {
	encodedData := base64.StdEncoding.EncodeToString(bytes)
	encodedKey := base64.StdEncoding.EncodeToString([]byte(dataKey))
	return encodedKey + SEPERATOR + encodedData + "\n"
}

func parseFromBytes(line []byte) (string, []byte, error) {
	idx := bytes.IndexByte(line, SEPERATOR[0])
	if idx == -1 {
		return "", nil, fmt.Errorf("invalid line format (missing separator): %q", string(line))
	}
	dataKeyBytes, err := base64.StdEncoding.DecodeString(string(line[:idx]))
	if err != nil {
		return "", nil, fmt.Errorf("failed to decode base64 key: %w", err)
	}
	dataKey := string(dataKeyBytes)
	protoBytes, err := base64.StdEncoding.DecodeString(string(line[idx+1:]))
	if err != nil {
		return "", nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	return dataKey, protoBytes, nil
}
