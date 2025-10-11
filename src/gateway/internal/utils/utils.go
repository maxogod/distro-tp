package utils

import (
	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	common_utils "github.com/maxogod/distro-tp/src/common/utils"
	"google.golang.org/protobuf/proto"
)

/* --- Transactions Data --- */

func TransactionFromRecord(record []string) *raw.Transaction {
	return &raw.Transaction{
		TransactionId: record[0],
		StoreId:       record[1],
		UserId:        record[4],
		FinalAmount:   common_utils.ParseFloatOrDefault(record[7]),
		CreatedAt:     record[8],
	}
}

func TransactionBatchFromList(list []*raw.Transaction) []byte {
	data, err := proto.Marshal(&raw.TransactionBatch{
		Transactions: list,
	})
	if err != nil {
		data = []byte{}
	}
	return data
}

/* --- Transaction Items Data --- */

func TransactionItemsFromRecord(record []string) *raw.TransactionItem {
	return &raw.TransactionItem{
		ItemId:    record[1],
		Quantity:  int32(common_utils.ParseIntOrDefault(record[2])),
		Subtotal:  common_utils.ParseFloatOrDefault(record[4]),
		CreatedAt: record[5],
	}
}

func TransactionItemsBatchFromList(list []*raw.TransactionItem) []byte {
	data, err := proto.Marshal(&raw.TransactionItemsBatch{
		TransactionItems: list,
	})
	if err != nil {
		data = []byte{}
	}
	return data
}

/* --- Users Data --- */

func UserFromRecord(record []string) *raw.User {
	return &raw.User{
		UserId:    int32(common_utils.ParseIntOrDefault(record[0])),
		Birthdate: record[2],
	}
}

func UserBatchFromList(list []*raw.User) []byte {
	data, err := proto.Marshal(&raw.UserBatch{
		Users: list,
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

func MenuItemFromRecord(record []string) *raw.MenuItem {
	return &raw.MenuItem{
		ItemId:   record[0],
		ItemName: record[1],
	}
}

func MenuItemBatchFromList(list []*raw.MenuItem) []byte {
	data, err := proto.Marshal(&raw.MenuItemsBatch{
		MenuItems: list,
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

func StoreFromRecord(record []string) *raw.Store {
	return &raw.Store{
		StoreId:   record[0],
		StoreName: record[1],
	}
}

func StoreBatchFromList(list []*raw.Store) []byte {
	data, err := proto.Marshal(&raw.StoreBatch{
		Stores: list,
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
