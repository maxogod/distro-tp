package file_handler

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
)

var log = logger.GetLogger()

const SEPERATOR = "#"

type fileHandler struct {
	openFiles map[string]*os.File
}

func NewFileHandler() FileHandler {
	return &fileHandler{
		openFiles: make(map[string]*os.File),
	}
}

// Reads protobuf bytes in batches from file.
func (fh *fileHandler) ReadData(
	path string,
	proto_ch chan []byte,
) error {
	log.Debugln("Reading from file:", path)
	defer close(proto_ch)

	file, err := os.Open(path)
	if err != nil {
		log.Errorf("failed to open file: %v", err)
		return err
	}
	fh.openFiles[path] = file
	defer file.Close()

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

func (fh *fileHandler) SaveData(path string, byte_ch chan []byte) error {
	outputFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	fh.openFiles[path] = outputFile
	writer := bufio.NewWriter(outputFile)
	defer outputFile.Close()

	for entry := range byte_ch {

		if len(entry) == 0 {
			continue
		}

		line := parseToString("", entry)
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
