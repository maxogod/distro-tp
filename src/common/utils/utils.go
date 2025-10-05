package utils

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

func AppendToCSVFile(path string, payload []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	f, openErr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if openErr != nil {
		return openErr
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("error closing file: %v\n", err)
		}
	}(f)

	if _, err := f.Write(payload); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func GetDataEnvelope(msg []byte) (*protocol.DataEnvelope, error) {

	dataBatch := &protocol.DataEnvelope{}
	err := proto.Unmarshal(msg, dataBatch)
	if err != nil {
		return nil, err
	}

	return dataBatch, nil
}

// This function creates a marshaled DataEnvelope containing the provided data, task type, and client ID.
// This simplifies the need to manually create and marshal DataEnvelope messages each time.
func CreateSerializedEnvelope(data proto.Message, taskType int32, clientID string) ([]byte, error) {
	payload, err := proto.Marshal(data)
	if err != nil {
		return nil, err
	}

	dataEnvelope := &protocol.DataEnvelope{
		TaskType: taskType,
		Payload:  payload,
		ClientId: clientID,
	}

	return proto.Marshal(dataEnvelope)
}

// UnmarshalPayload unmarshals payload into a proto.Message of type T.
func UnmarshalPayload[T proto.Message](payload []byte, msg T) (T, error) {
	err := proto.Unmarshal(payload, msg)
	if err != nil {
		return msg, err
	}
	return msg, nil
}
