package file_handler

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

const SEPERATOR = "@"

type fileHandler struct {
	batchSize int
	openFiles map[string]*os.File
}

func NewFileHandler(batchSize int) FileHandler {
	return &fileHandler{
		batchSize: batchSize,
		openFiles: make(map[string]*os.File),
	}
}

// Reads protobuf bytes in batches from file.
func (fh *fileHandler) ReadAsBatches(
	path string,
	batch_ch chan [][]byte,
) error {
	log.Debugln("Reading from file:", path)
	defer close(batch_ch)

	file, err := os.Open(path)
	if err != nil {
		log.Errorf("failed to open file: %v", err)
		return err
	}
	fh.openFiles[path] = file
	defer file.Close()

	scanner := bufio.NewScanner(file)
	batch := make([][]byte, 0, fh.batchSize)

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

		batch = append(batch, protoBytes)
		if len(batch) >= fh.batchSize {
			batch_ch <- batch
			batch = make([][]byte, 0, fh.batchSize)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Errorf("scanner error: %v", err)
	}

	if len(batch) > 0 {
		batch_ch <- batch
	}
	return nil
}

func (fh *fileHandler) SaveProtoData(path string, proto_ch chan proto.Message) error {
	outputFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	fh.openFiles[path] = outputFile
	writer := bufio.NewWriter(outputFile)

	for entry := range proto_ch {
		b, err := proto.Marshal(entry)
		if err != nil {
			return err
		}

		if len(b) == 0 {
			continue
		}

		line := parseToString("", b)
		if _, err := writer.WriteString(line); err != nil {
			return err
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	return nil
}

func (fh *fileHandler) SaveIndexedData(
	path string,
	dataKey string,
	updateFunc func(*[]byte),
) error {
	file, ok := fh.openFiles[path]
	var err error
	if !ok {
		file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed to open or create file: %w", err)
		}
		fh.openFiles[path] = file
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek start failed: %w", err)
	}

	scanner := bufio.NewScanner(file)
	var updatedLines []string
	var found bool

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		lineKey, protoBytes, err := parseFromBytes(line)
		if err != nil {
			return fmt.Errorf("parse line failed: %w", err)
		}

		if lineKey == dataKey {
			updateFunc(&protoBytes)
			newLine := parseToString(dataKey, protoBytes)
			updatedLines = append(updatedLines, newLine)
			found = true
		} else {
			updatedLines = append(updatedLines, string(line)+"\n")
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	// Append new record if not found
	if !found {
		data := []byte{}
		updateFunc(&data)
		newLine := parseToString(dataKey, data)
		updatedLines = append(updatedLines, newLine)
	}

	// Truncate and rewrite file
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek start failed: %w", err)
	}

	writer := bufio.NewWriter(file)
	for _, l := range updatedLines {
		if _, err := writer.WriteString(l); err != nil {
			return fmt.Errorf("failed to write line: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	return nil
}

func (fh *fileHandler) CloseFile(path string) error {
	file, ok := fh.openFiles[path]
	if !ok {
		return nil
	}
	delete(fh.openFiles, path)
	return file.Close()
}

func (fh *fileHandler) Close() {
	for _, file := range fh.openFiles {
		file.Close()
	}
}

func (fh *fileHandler) DeleteFile(path string) error {
	if err := fh.CloseFile(path); err != nil {
		return err
	}
	return os.Remove(path)
}

func parseToString(dataKey string, bytes []byte) string {
	encoded := base64.StdEncoding.EncodeToString(bytes)
	return dataKey + SEPERATOR + encoded + "\n"
}

func parseFromBytes(line []byte) (string, []byte, error) {
	idx := bytes.IndexByte(line, SEPERATOR[0])
	if idx == -1 {
		return "", nil, fmt.Errorf("invalid line format (missing separator): %q", string(line))
	}
	dataKey := string(line[:idx])
	protoBytes, err := base64.StdEncoding.DecodeString(string(line[idx+1:]))
	if err != nil {
		return "", nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	return dataKey, protoBytes, nil
}
