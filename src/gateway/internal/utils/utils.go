package utils

import (
	"strings"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	common_utils "github.com/maxogod/distro-tp/src/common/utils"
	"google.golang.org/protobuf/proto"
)

/* --- Transactions Data --- */

func TransactionFromRecord(record []string) proto.Message {
	return &raw.Transaction{
		TransactionId: record[0],
		StoreId:       record[1],
		UserId:        strings.Split(record[4], ".")[0],
		FinalAmount:   common_utils.ParseFloatOrDefault(record[7]),
		CreatedAt:     record[8],
	}
}

func TransactionBatchFromList(list []proto.Message) []byte {
	data, err := proto.Marshal(&raw.TransactionBatch{
		Transactions: castFromProtoMessageList[*raw.Transaction](list),
	})
	if err != nil {
		data = []byte{}
	}
	return data
}

/* --- Transaction Items Data --- */

func TransactionItemsFromRecord(record []string) proto.Message {
	return &raw.TransactionItem{
		ItemId:    record[1],
		Quantity:  int32(common_utils.ParseIntOrDefault(record[2])),
		Subtotal:  common_utils.ParseFloatOrDefault(record[4]),
		CreatedAt: record[5],
	}
}

func TransactionItemsBatchFromList(list []proto.Message) []byte {
	data, err := proto.Marshal(&raw.TransactionItemsBatch{
		TransactionItems: castFromProtoMessageList[*raw.TransactionItem](list),
	})
	if err != nil {
		data = []byte{}
	}
	return data
}

/* --- Users Data --- */

func UserFromRecord(record []string) proto.Message {
	return &raw.User{
		UserId:    record[0],
		Birthdate: record[2],
	}
}

func UserBatchFromList(list []proto.Message) []byte {
	data, err := proto.Marshal(&raw.UserBatch{
		Users: castFromProtoMessageList[*raw.User](list),
	})
	if err != nil {
		data = []byte{}
	}
	envelope := &protocol.ReferenceEnvelope{
		ReferenceType: int32(enum.Users),
		Payload:       data,
	}
	envelopedData, err := proto.Marshal(envelope)
	if err != nil {
		envelopedData = []byte{}
	}
	return envelopedData
}

/* --- Menu Items Data --- */

func MenuItemFromRecord(record []string) proto.Message {
	return &raw.MenuItem{
		ItemId:   record[0],
		ItemName: record[1],
	}
}

func MenuItemBatchFromList(list []proto.Message) []byte {
	data, err := proto.Marshal(&raw.MenuItemsBatch{
		MenuItems: castFromProtoMessageList[*raw.MenuItem](list),
	})
	if err != nil {
		data = []byte{}
	}
	envelope := &protocol.ReferenceEnvelope{
		ReferenceType: int32(enum.MenuItems),
		Payload:       data,
	}
	envelopedData, err := proto.Marshal(envelope)
	if err != nil {
		envelopedData = []byte{}
	}
	return envelopedData
}

/* --- Stores Data --- */

func StoreFromRecord(record []string) proto.Message {
	return &raw.Store{
		StoreId:   record[0],
		StoreName: record[1],
	}
}

func StoreBatchFromList(list []proto.Message) []byte {
	data, err := proto.Marshal(&raw.StoreBatch{
		Stores: castFromProtoMessageList[*raw.Store](list),
	})
	if err != nil {
		data = []byte{}
	}
	envelope := &protocol.ReferenceEnvelope{
		ReferenceType: int32(enum.Stores),
		Payload:       data,
	}
	envelopedData, err := proto.Marshal(envelope)
	if err != nil {
		envelopedData = []byte{}
	}
	return envelopedData
}

func castFromProtoMessageList[T proto.Message](list []proto.Message) []T {
	castedList := make([]T, len(list))
	for i, item := range list {
		castedList[i] = item.(T)
	}
	return castedList
}
