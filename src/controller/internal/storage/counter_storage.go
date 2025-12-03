package storage

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
	basePath string
}

func NewCounterStorage(basePath string) CounterStorage {
	return &diskCounterStorage{basePath: basePath}
}

// GetClientIds returns the list of client IDs stored in the storage, based on the files in the storage directory
func (cs *diskCounterStorage) GetClientIds() ([]string, error) {
	logger.Logger.Debugf("Listing clients in storage at %s", cs.basePath)

	if err := os.MkdirAll(cs.basePath, 0o755); err != nil {
		return nil, fmt.Errorf("unable to create counter storage dir: %w", err)
	}

	counterFiles, err := cs.getFiles()
	if err != nil {
		return nil, fmt.Errorf("unable to get counter files: %w", err)
	}

	clients := make([]string, 0, len(counterFiles))
	for _, counterFile := range counterFiles {
		clients = append(clients, getBaseFileName(counterFile))
	}

	logger.Logger.Debugf("Found %d clients in storage: %v", len(clients), clients)
	return clients, nil
}

// ReadClientCounters returns the list of counters for the given client
func (cs *diskCounterStorage) ReadClientCounters(clientID string) ([]*protocol.MessageCounter, error) {
	logger.Logger.Debugf("Reading counters for client %s", clientID)

	path := cs.getFilePath(clientID)
	counterFile, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("counter file for client %s does not exist: %v", clientID, err)
		}
		return nil, fmt.Errorf("failed to open counter file: %w", err)
	}
	defer counterFile.Close()

	fileScanner := bufio.NewScanner(counterFile)
	counters := make([]*protocol.MessageCounter, 0)

	for fileScanner.Scan() {
		counterLine := fileScanner.Bytes()
		if len(counterLine) == 0 {
			continue
		}

		// If we get an error, we return the counters we have read so far
		counter, getErr := cs.getCounterFromLine(clientID, counterLine, counters, err)
		if getErr != nil {
			logger.Logger.Warnf("failed to read counter for client %s: %v", clientID, getErr)
			return counters, getErr
		}
		counters = append(counters, counter)
	}

	if scanErr := fileScanner.Err(); scanErr != nil {
		return counters, fmt.Errorf("failed scanning counter file: %w", scanErr)
	}

	logger.Logger.Debugf("read %d counters for client %s", len(counters), clientID)
	return counters, nil
}

// AppendCounter appends the given counter to the counter file for the given client
func (cs *diskCounterStorage) AppendCounter(clientID string, messageCounter *protocol.MessageCounter) error {
	if messageCounter == nil {
		return fmt.Errorf("cannot persist nil counter for client %s", clientID)
	}

	if err := os.MkdirAll(cs.basePath, 0o755); err != nil {
		return fmt.Errorf("failed to create storage dir: %w", err)
	}

	counterBytes, err := proto.Marshal(messageCounter)
	if err != nil {
		return fmt.Errorf("failed to marshal counter: %w", err)
	}

	encodedCounter := base64.StdEncoding.EncodeToString(counterBytes) + "\n"
	file, err := os.OpenFile(cs.getFilePath(clientID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		logger.Logger.Errorf("failed to open counter file: %v", err)
		return fmt.Errorf("failed to open counter file: %w", err)
	}
	defer file.Close()

	if _, err = file.WriteString(encodedCounter); err != nil {
		return fmt.Errorf("failed to write counter: %w", err)
	}
	if err = file.Sync(); err != nil {
		return fmt.Errorf("failed to sync counter file: %w", err)
	}

	return nil
}

// RemoveClient removes the counter file for the given client
func (cs *diskCounterStorage) RemoveClient(clientID string) error {
	if err := os.Remove(cs.getFilePath(clientID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove counter file: %w", err)
	}
	logger.Logger.Debugf("removed counters for client %s", clientID)
	return nil
}

// InitializeClientCounter creates a new counter file for the given client with the given task type
func (cs *diskCounterStorage) InitializeClientCounter(clientID string, taskType enum.TaskType) error {
	if err := os.MkdirAll(cs.basePath, 0o755); err != nil {
		return fmt.Errorf("failed to create storage dir: %w", err)
	}

	if err := cs.touchFiles(clientID); err != nil {
		return err
	}

	counter := &protocol.MessageCounter{
		ClientId: clientID,
		TaskType: int32(taskType),
	}

	counterBytes, err := proto.Marshal(counter)
	if err != nil {
		return fmt.Errorf("failed to marshal initial counter: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(counterBytes) + "\n"

	return os.WriteFile(cs.getFilePath(clientID), []byte(encoded), 0o644)
}

// ==== Helper functions ====

// rewriteClientFile replaces the counter file for the given client with a new one containing the given counters
func (cs *diskCounterStorage) rewriteClientFile(clientID string, counters []*protocol.MessageCounter) error {
	if err := os.MkdirAll(cs.basePath, 0o755); err != nil {
		return fmt.Errorf("failed to prepare storage dir: %w", err)
	}

	path := cs.getFilePath(clientID)
	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open temp counter file: %w", err)
	}

	writer := bufio.NewWriter(file)
	for _, counter := range counters {
		if err = cs.writeCounter(counter, file, err, writer); err != nil {
			return err
		}
	}

	if err = writer.Flush(); err != nil {
		file.Close()
		return fmt.Errorf("failed to flush rewrite file: %w", err)
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("failed to sync rewrite file: %w", err)
	}
	file.Close()

	if err = os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("failed to replace counter file: %w", err)
	}

	logger.Logger.Debugf("rewritten counters for client %s", clientID)
	return nil
}

func (cs *diskCounterStorage) getCounterFromLine(clientID string, counterLine []byte, counters []*protocol.MessageCounter, err error) (*protocol.MessageCounter, error) {
	decoded, decodeErr := base64.StdEncoding.DecodeString(string(counterLine))
	if decodeErr != nil {
		return nil, cs.handleCorruption(clientID, counters, decodeErr, "failed to decode counter entry")
	}

	counter := &protocol.MessageCounter{}
	if err = proto.Unmarshal(decoded, counter); err != nil {
		return nil, cs.handleCorruption(clientID, counters, err, "failed to unmarshal counter entry")
	}
	return counter, nil
}

func (cs *diskCounterStorage) writeCounter(counter *protocol.MessageCounter, file *os.File, err error, writer *bufio.Writer) error {
	counterBytes, marshalErr := proto.Marshal(counter)
	if marshalErr != nil {
		file.Close()
		return fmt.Errorf("failed to marshal counter during rewrite: %w", marshalErr)
	}

	encodedCounter := base64.StdEncoding.EncodeToString(counterBytes) + "\n"
	if _, err = writer.WriteString(encodedCounter); err != nil {
		file.Close()
		return fmt.Errorf("failed to rewrite counter entry: %w", err)
	}
	return nil
}

func (cs *diskCounterStorage) getFilePath(clientID string) string {
	return filepath.Join(cs.basePath, clientID+counterExtension)
}

func (cs *diskCounterStorage) getFiles() ([]string, error) {
	pattern := filepath.Join(cs.basePath, "*"+counterExtension)
	return filepath.Glob(pattern)
}

func (cs *diskCounterStorage) handleCorruption(clientID string, counters []*protocol.MessageCounter, cause error, msg string) error {
	logger.Logger.Warnf("[%s] counter file corrupted, rewriting file: %v", clientID, cause)
	if writeErr := cs.rewriteClientFile(clientID, counters); writeErr != nil {
		return fmt.Errorf("[%s] failed to rewrite corrupted counter file: %w", clientID, writeErr)
	}
	return fmt.Errorf("%s: %w", msg, cause)
}

func (cs *diskCounterStorage) touchFiles(clientID string) error {
	if err := os.MkdirAll(cs.basePath, 0o755); err != nil {
		return fmt.Errorf("failed to create storage dir: %w", err)
	}

	if _, err := os.Stat(cs.getFilePath(clientID)); errors.Is(err, os.ErrNotExist) {
		file, createErr := os.OpenFile(cs.getFilePath(clientID), os.O_CREATE|os.O_WRONLY, 0o644)
		if createErr != nil {
			return fmt.Errorf("failed to create counter file: %w", createErr)
		}
		file.Close()
	} else if err != nil {
		return fmt.Errorf("failed to stat counter file: %w", err)
	}

	return nil
}

func getBaseFileName(path string) string {
	file := filepath.Base(path)
	fileName := strings.TrimSuffix(file, counterExtension)
	return fileName
}
