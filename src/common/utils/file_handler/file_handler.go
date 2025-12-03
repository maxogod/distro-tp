package file_handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/logger"
)

const FINISH_DATA_BYTE = 0xff
const SYNC_DATA_BYTE = 0xfe

type fileHandler struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewFileHandler() FileHandler {
	ctx, cancel := context.WithCancel(context.Background())
	return &fileHandler{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (fh *fileHandler) InitReader(path string) (chan []byte, error) {
	return fh.readFile(path, -1)
}

func (fh *fileHandler) InitReadUpTo(path string, amount int) (chan []byte, error) {
	return fh.readFile(path, amount)
}

func (fh *fileHandler) GetFileSize(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		logger.Logger.Errorf("failed to open file: %v", err)
		return 0, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		logger.Logger.Errorf("failed to get file info: %v", err)
		return 0, err
	}

	limitedReader := io.LimitReader(file, fileInfo.Size())
	scanner := bufio.NewScanner(limitedReader)

	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		logger.Logger.Errorf("failed to scan file: %v", err)
		return 0, err
	}

	return lineCount, nil
}

func (fh *fileHandler) InitWriter(path string) (*FileWriter, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0777); err != nil {
		logger.Logger.Errorf("failed to create output directory: %v", err)
		return nil, err
	}
	outputFile, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)

	writeCh := make(chan []byte)
	syncCh := make(chan bool)

	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	writer := bufio.NewWriter(outputFile)

	go func() {
		defer func() {
			writer.Flush()
			outputFile.Sync()
			outputFile.Close()
			syncCh <- true
			close(syncCh)
			close(writeCh)
		}()
		for entry := range writeCh {
			if bytes.Equal(entry, []byte{FINISH_DATA_BYTE}) {
				break
			}
			if bytes.Equal(entry, []byte{SYNC_DATA_BYTE}) {
				writer.Flush()
				outputFile.Sync()
				syncCh <- true
				continue
			}

			select {
			case <-fh.ctx.Done():
				logger.Logger.Debugf("Context cancelled, stopping file write")
				return
			default:
			}

			if len(entry) == 0 {
				continue
			}

			line := parseToString(entry)
			if _, err := writer.WriteString(line); err != nil {
				logger.Logger.Errorf("failed to write to file: %v", err)
				return
			}

		}
	}()

	return &FileWriter{
		storeCh: writeCh,
		syncCh:  syncCh,
	}, nil
}

func (fh *fileHandler) IsFilePresent(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func (fh *fileHandler) Close() {
	fh.cancel()
}

func (fh *fileHandler) DeleteFile(path string) {
	os.Remove(path)
}

func (fh *fileHandler) RenameFile(oldPath string, newPath string) error {
	return os.Rename(oldPath, newPath)
}

func (fh *fileHandler) readFile(path string, limit int) (chan []byte, error) {
	readFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	reachCh := make(chan []byte)
	scanner := bufio.NewScanner(readFile)

	go func() {
		defer readFile.Close()
		defer close(reachCh)

		count := 0
		for scanner.Scan() {
			// Check if context is cancelled
			select {
			case <-fh.ctx.Done():
				logger.Logger.Debugf("Context cancelled, stopping file read")
				return
			default:
			}

			// Check limit (if limit is -1, read all)
			if limit > 0 && count >= limit {
				break
			}

			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			protoBytes, err := parseEncodedBytes(line)
			if err != nil {
				logger.Logger.Errorf("failed to parse from bytes: %v", err)
				continue
			}

			// Try to send data, but also check for context cancellation
			select {
			case reachCh <- protoBytes:
				count++
			case <-fh.ctx.Done():
				logger.Logger.Debugf("Context cancelled while sending data")
				return
			}
		}

		if err := scanner.Err(); err != nil {
			logger.Logger.Errorf("scanner error: %v", err)
		}
	}()

	return reachCh, nil
}

func parseToString(bytes []byte) string {
	encodedData := base64.StdEncoding.EncodeToString(bytes)
	return encodedData + "\n"
}

func parseEncodedBytes(line []byte) ([]byte, error) {
	protoBytes, err := base64.StdEncoding.DecodeString(string(line))
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	return protoBytes, nil
}
