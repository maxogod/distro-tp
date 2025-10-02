package cache

import (
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"google.golang.org/protobuf/proto"
)

func createDataBatchFromJoined[T proto.Message](taskType enum.TaskType, items []T, makeDataBatchMsg func([]T) proto.Message) (*data_batch.DataBatch, error) {
	batch := makeDataBatchMsg(items)

	payloadBytes, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	return &data_batch.DataBatch{
		TaskType: int32(taskType),
		Payload:  payloadBytes,
	}, nil
}

func CreateJoinStoreTPVBatch(taskType enum.TaskType, joinedData []*joined.JoinStoreTPV) (*data_batch.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joinedData,
		func(items []*joined.JoinStoreTPV) proto.Message {
			return &joined.JoinStoreTPVBatch{Items: items}
		},
	)
}

func CreateBestSellingBatch(taskType enum.TaskType, joinedData []*joined.JoinBestSellingProducts) (*data_batch.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joinedData,
		func(items []*joined.JoinBestSellingProducts) proto.Message {
			return &joined.JoinBestSellingProductsBatch{Items: items}
		},
	)
}

func CreateMostProfitsBatch(taskType enum.TaskType, joinedData []*joined.JoinMostProfitsProducts) (*data_batch.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joinedData,
		func(items []*joined.JoinMostProfitsProducts) proto.Message {
			return &joined.JoinMostProfitsProductsBatch{Items: items}
		},
	)
}

func CreateMostPurchasesUserBatch(taskType enum.TaskType, joinedData []*joined.JoinMostPurchasesUser) (*data_batch.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joinedData,
		func(items []*joined.JoinMostPurchasesUser) proto.Message {
			return &joined.JoinMostPurchasesUserBatch{Users: items}
		},
	)
}
