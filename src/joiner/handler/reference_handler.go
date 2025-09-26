package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"google.golang.org/protobuf/proto"
)

type DoneMsgHandler func(queueName string, taskType models.TaskType) error
type ReferenceBatchMsg = protocol.ReferenceQueueMessage_ReferenceBatch
type DoneMsg = protocol.ReferenceQueueMessage_Done

type ReferenceHandler struct {
	handlerDone DoneMsgHandler
	queue       string
	storeDir    string
}

func NewReferenceHandler(handlerDone DoneMsgHandler, queueName, storeDir string) *ReferenceHandler {
	return &ReferenceHandler{handlerDone: handlerDone, queue: queueName, storeDir: storeDir}
}

func (h *ReferenceHandler) HandleReferenceQueueMessage(msgBody []byte) error {
	var refMsg protocol.ReferenceQueueMessage
	if err := proto.Unmarshal(msgBody, &refMsg); err != nil {
		return err
	}

	switch payload := refMsg.Payload.(type) {
	case *ReferenceBatchMsg:
		return cache.StoreReferenceData(h.storeDir, payload.ReferenceBatch)
	case *DoneMsg:
		return h.handlerDone(h.queue, models.TaskType(payload.Done.TaskType))
	default:
		return fmt.Errorf("unknown message type")
	}
}
