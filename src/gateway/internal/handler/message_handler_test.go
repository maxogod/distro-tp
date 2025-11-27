package handler

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"google.golang.org/protobuf/proto"
)

func TestIsDuplicateProcessedData(t *testing.T) {
	mh := &messageHandler{processedSignatures: make(map[string]struct{})}

	envelope := &protocol.DataEnvelope{
		ClientId:       "client-1",
		TaskType:       int32(enum.T2),
		SequenceNumber: 3,
		Payload:        []byte("data"),
	}

	if mh.isDuplicateProcessedData(envelope) {
		t.Fatalf("first occurrence should not be considered duplicate")
	}

	duplicate := proto.Clone(envelope).(*protocol.DataEnvelope)
	if !mh.isDuplicateProcessedData(duplicate) {
		t.Fatalf("second occurrence should be considered duplicate")
	}
}

func TestIsDuplicateProcessedDataDifferentPayload(t *testing.T) {
	mh := &messageHandler{processedSignatures: make(map[string]struct{})}

	envelope := &protocol.DataEnvelope{
		ClientId:       "client-1",
		TaskType:       int32(enum.T2),
		SequenceNumber: 5,
		Payload:        []byte("first"),
	}

	if mh.isDuplicateProcessedData(envelope) {
		t.Fatalf("first occurrence should not be duplicate")
	}

	differentPayload := &protocol.DataEnvelope{
		ClientId:       "client-1",
		TaskType:       int32(enum.T2),
		SequenceNumber: 5,
		Payload:        []byte("second"),
	}

	if mh.isDuplicateProcessedData(differentPayload) {
		t.Fatalf("different payload should not be treated as duplicate")
	}
}
