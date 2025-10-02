package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/joiner/business"
)

type ReferenceHandler struct {
	// TODO: CHANGE WITH CACHE SERVICE!!!
	joinerService *business.JoinerService
	taskHandlers  map[enum.RefDatasetType]func([]byte) error
}

func NewReferenceHandler(
	filterService *business.JoinerService,
	queueHandler *MessageHandler) *ReferenceHandler {
	rh := &ReferenceHandler{
		joinerService: filterService,
	}

	rh.taskHandlers = map[enum.RefDatasetType]func([]byte) error{
		enum.MenuItems: rh.handleMenuItems,
		enum.Stores:    rh.handleStores,
		enum.Users:     rh.handleUsers,
	}

	return rh
}

func (rh *ReferenceHandler) HandleReference(taskType enum.RefDatasetType, payload []byte) error {
	handler, exists := rh.taskHandlers[taskType]
	if !exists {
		return fmt.Errorf("unknown task type: %d", taskType)
	}
	return handler(payload)
}

func (rh *ReferenceHandler) handleMenuItems(payload []byte) error {

	refData, err := utils.UnmarshalPayload(payload, &raw.MenuItemBatch{})
	if err != nil {
		return err
	}

	// ===================
	// ADD CACHE LOGIC HERE
	// ===================

	return nil
}

func (rh *ReferenceHandler) handleStores(payload []byte) error {

	refData, err := utils.UnmarshalPayload(payload, &raw.StoreBatch{})
	if err != nil {
		return err
	}

	// ===================
	// ADD CACHE LOGIC HERE
	// ===================

	return nil
}

func (rh *ReferenceHandler) handleUsers(payload []byte) error {

	refData, err := utils.UnmarshalPayload(payload, &raw.MenuItemBatch{})
	if err != nil {
		return err
	}

	// ===================
	// ADD CACHE LOGIC HERE
	// ===================

	return nil
}
