package utils

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

func GetDataEnvelope(msg []byte) (*protocol.DataEnvelope, error) {

	dataBatch := &protocol.DataEnvelope{}
	err := proto.Unmarshal(msg, dataBatch)
	if err != nil {
		return nil, err
	}

	return dataBatch, nil
}

// CreateSerializedEnvelope creates a marshaled DataEnvelope containing the provided data, task type, and client ID.
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

func ParseSemester(semester string) (int, int) {
	var year int
	var half int

	// Parse the year and half-year
	fmt.Sscanf(semester, "%d-H%d", &year, &half)

	return year, half
}

func ToProtoMessage[T proto.Message](data T) *proto.Message {
	msg := proto.Message(data)
	return &msg
}

func FromProtoMessage[T proto.Message](msg *proto.Message) T {
	return (*msg).(T)
}
