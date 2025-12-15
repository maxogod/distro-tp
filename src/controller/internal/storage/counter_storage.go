package storage

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

const (
	counterFolder    = "storage"
	counterExtension = ".counter"
)

type diskCounterStorage struct {
	basePath  string
	openFiles sync.Map
}

func NewCounterStorage(basePath string) (CounterStorage, error) {
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}
	return &diskCounterStorage{
		basePath: basePath,
	}, nil
}

func (cs *diskCounterStorage) GetClientIds() ([]string, error) {
	logger.Logger.Debugf("Listing clients in storage at %s", cs.basePath)

	counterFiles, err := cs.getFiles()
	if err != nil {
		return nil, fmt.Errorf("unable to get counter files: %w", err)
	}

	clients := make([]string, 0, len(counterFiles))
	for _, counterFile := range counterFiles {
		file := filepath.Base(counterFile)
		fileName := strings.TrimSuffix(file, counterExtension)
		clients = append(clients, fileName)
	}

	logger.Logger.Debugf("Found %d clients in storage: %v", len(clients), clients)
	return clients, nil
}

func (cs *diskCounterStorage) ReadClientCounters(clientID string) ([]*protocol.MessageCounter, error) {
	logger.Logger.Debugf("Reading counters for client %s", clientID)

	val, exists := cs.openFiles.Load(clientID)
	var file *os.File
	if !exists {
		var err error
		file, err = os.OpenFile(cs.getFilePath(clientID), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("counter file for client %s does not exist: %v", clientID, err)
			}
			return nil, fmt.Errorf("failed to open counter file: %w", err)
		}
		cs.openFiles.Store(clientID, file)
	} else {
		var ok bool
		file, ok = val.(*os.File)
		if !ok {
			return nil, fmt.Errorf("invalid file handle for client %s", clientID)
		}
	}

	// Seek to beginning for reading
	if _, seekErr := file.Seek(0, 0); seekErr != nil {
		return nil, fmt.Errorf("failed to seek to beginning of file: %w", seekErr)
	}

	fileScanner := bufio.NewScanner(file)
	counters := make([]*protocol.MessageCounter, 0)

	for fileScanner.Scan() {
		counterLine := fileScanner.Bytes()
		if len(counterLine) == 0 {
			continue
		}

		decoded, decodeErr := base64.StdEncoding.DecodeString(string(counterLine))
		if decodeErr != nil {
			logger.Logger.Warnf("failed to decode counter for client %s: %v", clientID, decodeErr)
			return counters, fmt.Errorf("failed to decode counter entry: %w", decodeErr)
		}

		counter := &protocol.MessageCounter{}
		if err := proto.Unmarshal(decoded, counter); err != nil {
			logger.Logger.Warnf("failed to unmarshal counter for client %s: %v", clientID, err)
			return counters, fmt.Errorf("failed to unmarshal counter entry: %w", err)
		}
		counters = append(counters, counter)
	}

	if scanErr := fileScanner.Err(); scanErr != nil {
		return counters, fmt.Errorf("failed scanning counter file: %w", scanErr)
	}

	logger.Logger.Debugf("read %d counters for client %s", len(counters), clientID)
	return counters, nil
}

func (cs *diskCounterStorage) AppendCounters(clientID string, messageCounters []*protocol.MessageCounter, taskType enum.TaskType) error {
	if len(messageCounters) == 0 {
		return nil
	}

	for _, counter := range messageCounters {
		if counter == nil {
			return fmt.Errorf("cannot persist nil counter for client %s", clientID)
		}
	}

	val, exists := cs.openFiles.Load(clientID)
	var file *os.File
	if !exists {
		var err error
		file, err = os.OpenFile(cs.getFilePath(clientID), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			logger.Logger.Errorf("failed to open counter file: %v", err)
			return fmt.Errorf("failed to open counter file: %w", err)
		}
		cs.openFiles.Store(clientID, file)

		// if new file write initial counter
		stat, statErr := file.Stat()
		if statErr == nil && stat.Size() == 0 {
			initialCounter := &protocol.MessageCounter{
				ClientId: clientID,
				TaskType: int32(taskType),
			}
			initialBytes, marshalErr := proto.Marshal(initialCounter)
			if marshalErr != nil {
				return fmt.Errorf("failed to marshal initial counter: %w", marshalErr)
			}
			encodedInitial := base64.StdEncoding.EncodeToString(initialBytes) + "\n"
			if _, writeErr := file.WriteString(encodedInitial); writeErr != nil {
				return fmt.Errorf("failed to write initial counter: %w", writeErr)
			}
		}
	} else {
		var ok bool
		file, ok = val.(*os.File)
		if !ok {
			return fmt.Errorf("invalid file handle for client %s", clientID)
		}
	}

	writer := bufio.NewWriter(file)
	for _, messageCounter := range messageCounters {
		counterBytes, err := proto.Marshal(messageCounter)
		if err != nil {
			return fmt.Errorf("failed to marshal counter: %w", err)
		}

		encodedCounter := base64.StdEncoding.EncodeToString(counterBytes) + "\n"
		if _, err = writer.WriteString(encodedCounter); err != nil {
			return fmt.Errorf("failed to write counter: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush counter file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync counter file: %w", err)
	}

	return nil
}

func (cs *diskCounterStorage) RemoveClient(clientID string) error {
	// Close the file if open
	if val, exists := cs.openFiles.Load(clientID); exists {
		if file, ok := val.(*os.File); ok {
			file.Close()
		}
		cs.openFiles.Delete(clientID)
	}

	if err := os.Remove(cs.getFilePath(clientID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove counter file: %w", err)
	}
	logger.Logger.Debugf("removed counters for client %s", clientID)
	return nil
}


func (cs *diskCounterStorage) getFilePath(clientID string) string {
	return filepath.Join(cs.basePath, clientID+counterExtension)
}

func (cs *diskCounterStorage) getFiles() ([]string, error) {
	pattern := filepath.Join(cs.basePath, "*"+counterExtension)
	return filepath.Glob(pattern)
}
