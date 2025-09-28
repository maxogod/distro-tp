package test_helpers

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func prepareDataBatch[T any](
	t *testing.T,
	taskType models.TaskType,
	items []*T,
	createContainer func([]*T) proto.Message,
) *protocol.DataBatch {
	t.Helper()

	container := createContainer(items)

	payload, err := proto.Marshal(container)
	assert.NoError(t, err)

	return &protocol.DataBatch{
		TaskType: int32(taskType),
		Payload:  payload,
	}
}

func PrepareStoreTPVBatch(t *testing.T, tpvs []*protocol.StoreTPV, taskType models.TaskType) *protocol.DataBatch {
	return prepareDataBatch(t, taskType, tpvs, func(items []*protocol.StoreTPV) proto.Message {
		return &protocol.StoresTPV{Items: items}
	})
}

func PrepareBestSellingBatch(t *testing.T, records []*protocol.BestSellingProducts, taskType models.TaskType) *protocol.DataBatch {
	return prepareDataBatch(t, taskType, records, func(items []*protocol.BestSellingProducts) proto.Message {
		return &protocol.BestSellingProductsBatch{Items: items}
	})
}

func PrepareMostProfitsBatch(t *testing.T, records []*protocol.MostProfitsProducts, taskType models.TaskType) *protocol.DataBatch {
	return prepareDataBatch(t, taskType, records, func(items []*protocol.MostProfitsProducts) proto.Message {
		return &protocol.MostProfitsProductsBatch{Items: items}
	})
}
