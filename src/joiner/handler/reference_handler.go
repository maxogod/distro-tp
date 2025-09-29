package handler

import (
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/joiner/cache"
	"google.golang.org/protobuf/proto"
)

type DoneMsgHandler func(queueName string, taskType enum.TaskType) error

type ReferenceHandler struct {
	handlerDone     DoneMsgHandler
	queue           string
	storeDir        string
	refDatasetStore *cache.ReferenceDatasetStore
}

func NewReferenceHandler(handlerDone DoneMsgHandler, queueName string, referenceDatasetStore *cache.ReferenceDatasetStore) *ReferenceHandler {
	return &ReferenceHandler{
		handlerDone:     handlerDone,
		queue:           queueName,
		refDatasetStore: referenceDatasetStore,
	}
}

func (h *ReferenceHandler) HandleReferenceQueueMessage(msgBody []byte) error {
	var refMsg data_batch.DataBatch
	if err := proto.Unmarshal(msgBody, &refMsg); err != nil {
		return err
	}

	if refMsg.Done {
		return h.handlerDone(h.queue, enum.TaskType(refMsg.TaskType))
	} else {
		return h.refDatasetStore.StoreReferenceData(&refMsg, h.queue)
	}
}
