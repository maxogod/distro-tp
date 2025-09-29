package cache

import (
	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	"google.golang.org/protobuf/proto"
)

func createDataBatchFromJoined[T proto.Message](taskType models.TaskType, items []T, makeDataBatchMsg func([]T) proto.Message) (*protocol.DataBatch, error) {
	batch := makeDataBatchMsg(items)

	payloadBytes, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	return &protocol.DataBatch{
		TaskType: int32(taskType),
		Payload:  payloadBytes,
	}, nil
}

func CreateJoinStoreTPVBatch(taskType models.TaskType, joined []*protocol.JoinStoreTPV) (*protocol.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joined,
		func(items []*protocol.JoinStoreTPV) proto.Message {
			return &protocol.JoinStoreTPVBatch{Items: items}
		},
	)
}

func CreateBestSellingBatch(taskType models.TaskType, joined []*protocol.JoinBestSellingProducts) (*protocol.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joined,
		func(items []*protocol.JoinBestSellingProducts) proto.Message {
			return &protocol.JoinBestSellingProductsBatch{Items: items}
		},
	)
}

func CreateMostProfitsBatch(taskType models.TaskType, joined []*protocol.JoinMostProfitsProducts) (*protocol.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joined,
		func(items []*protocol.JoinMostProfitsProducts) proto.Message {
			return &protocol.JoinMostProfitsProductsBatch{Items: items}
		},
	)
}

func CreateMostPurchasesUserBatch(taskType models.TaskType, joined []*protocol.JoinMostPurchasesUser) (*protocol.DataBatch, error) {
	return createDataBatchFromJoined(
		taskType,
		joined,
		func(items []*protocol.JoinMostPurchasesUser) proto.Message {
			return &protocol.JoinMostPurchasesUserBatch{Users: items}
		},
	)
}
