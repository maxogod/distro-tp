package handler

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/cache"
)

type ReferenceHandler struct {
	joinerService *business.JoinerService
	taskHandlers  map[enum.RefDatasetType]func([]byte) error
	refStore      *cache.ReferenceDatasetStore
}

func NewReferenceHandler(filterService *business.JoinerService, refStore *cache.ReferenceDatasetStore) *ReferenceHandler {
	rh := &ReferenceHandler{
		joinerService: filterService,
		refStore:      refStore,
	}

	rh.taskHandlers = map[enum.RefDatasetType]func([]byte) error{
		enum.MenuItems: rh.handleMenuItems,
		enum.Stores:    rh.handleStores,
		enum.Users:     rh.handleUsers,
	}

	return rh
}

func (rh *ReferenceHandler) HandleReference(referenceType enum.RefDatasetType, payload []byte) error {
	handler, exists := rh.taskHandlers[referenceType]
	if !exists {
		return fmt.Errorf("unknown ref type: %d", referenceType)
	}
	return handler(payload)
}

func (rh *ReferenceHandler) handleMenuItems(payload []byte) error {

	refData, err := utils.UnmarshalPayload(payload, &raw.MenuItemBatch{})
	if err != nil {
		return err
	}

	return rh.refStore.PersistMenuItemsBatch(refData)
}

func (rh *ReferenceHandler) handleStores(payload []byte) error {

	refData, err := utils.UnmarshalPayload(payload, &raw.StoreBatch{})
	if err != nil {
		return err
	}

	return rh.refStore.PersistStoresBatch(refData)
}

func (rh *ReferenceHandler) handleUsers(payload []byte) error {

	refData, err := utils.UnmarshalPayload(payload, &raw.UserBatch{})
	if err != nil {
		return err
	}

	return rh.refStore.PersistUsersBatch(refData)
}
