package test_helpers

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func prepareDataBatch[T any](
	t *testing.T,
	taskType enum.TaskType,
	items []*T,
	createContainer func([]*T) proto.Message,
) *data_batch.DataBatch {
	t.Helper()

	container := createContainer(items)

	payload, err := proto.Marshal(container)
	assert.NoError(t, err)

	return &data_batch.DataBatch{
		TaskType: int32(taskType),
		Payload:  payload,
	}
}

func PrepareStoreTPVBatch(t *testing.T, tpvs []*reduced.StoreTPV, taskType enum.TaskType) *data_batch.DataBatch {
	return prepareDataBatch(t, taskType, tpvs, func(items []*reduced.StoreTPV) proto.Message {
		return &reduced.StoresTPV{Items: items}
	})
}

func PrepareBestSellingBatch(t *testing.T, records []*reduced.BestSellingProducts, taskType enum.TaskType) *data_batch.DataBatch {
	return prepareDataBatch(t, taskType, records, func(items []*reduced.BestSellingProducts) proto.Message {
		return &reduced.BestSellingProductsBatch{Items: items}
	})
}

func PrepareMostProfitsBatch(t *testing.T, records []*reduced.MostProfitsProducts, taskType enum.TaskType) *data_batch.DataBatch {
	return prepareDataBatch(t, taskType, records, func(items []*reduced.MostProfitsProducts) proto.Message {
		return &reduced.MostProfitsProductsBatch{Items: items}
	})
}

func PrepareMostPurchasesUserBatch(t *testing.T, tpvs []*reduced.MostPurchasesUser, taskType enum.TaskType) *data_batch.DataBatch {
	return prepareDataBatch(t, taskType, tpvs, func(users []*reduced.MostPurchasesUser) proto.Message {
		return &reduced.MostPurchasesUserBatch{Users: users}
	})
}
