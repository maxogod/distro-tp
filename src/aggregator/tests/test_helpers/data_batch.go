package test_helpers

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/joined"
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

func PrepareJoinBestSellingBatch(t *testing.T, records []*joined.JoinBestSellingProducts, taskType enum.TaskType) *data_batch.DataBatch {
	return prepareDataBatch(t, taskType, records, func(items []*joined.JoinBestSellingProducts) proto.Message {
		return &joined.JoinBestSellingProductsBatch{Items: items}
	})
}

func PrepareJoinMostProfitsBatch(t *testing.T, records []*joined.JoinMostProfitsProducts, taskType enum.TaskType) *data_batch.DataBatch {
	return prepareDataBatch(t, taskType, records, func(items []*joined.JoinMostProfitsProducts) proto.Message {
		return &joined.JoinMostProfitsProductsBatch{Items: items}
	})
}

func PrepareJoinMostPurchasesUserBatch(t *testing.T, tpvs []*joined.JoinMostPurchasesUser, taskType enum.TaskType) *data_batch.DataBatch {
	return prepareDataBatch(t, taskType, tpvs, func(users []*joined.JoinMostPurchasesUser) proto.Message {
		return &joined.JoinMostPurchasesUserBatch{Users: users}
	})
}
