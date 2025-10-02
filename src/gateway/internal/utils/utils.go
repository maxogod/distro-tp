package utils

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	common_utils "github.com/maxogod/distro-tp/src/common/utils"
	"google.golang.org/protobuf/proto"
)

/* --- Transactions Data --- */

func TransactionFromRecord(record []string) *raw.Transaction {
	return &raw.Transaction{
		TransactionId:   record[0],
		StoreId:         int64(common_utils.ParseIntOrDefault(record[1])),
		PaymentMethod:   int32(common_utils.ParseIntOrDefault(record[2])),
		VoucherId:       int64(common_utils.ParseIntOrDefault(record[3])),
		UserId:          int64(common_utils.ParseIntOrDefault(record[4])),
		OriginalAmount:  common_utils.ParseFloatOrDefault(record[5]),
		DiscountApplied: common_utils.ParseFloatOrDefault(record[6]),
		FinalAmount:     common_utils.ParseFloatOrDefault(record[7]),
		CreatedAt:       record[8],
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

func TransactionItemsFromRecord(record []string) *raw.TransactionItems {
	return &raw.TransactionItems{
		TransactionId: record[0],
		ItemId:        int64(common_utils.ParseIntOrDefault(record[1])),
		Quantity:      int32(common_utils.ParseIntOrDefault(record[2])),
		UnitPrice:     common_utils.ParseFloatOrDefault(record[3]),
		Subtotal:      common_utils.ParseFloatOrDefault(record[4]),
		CreatedAt:     record[5],
	}
}

func TransactionItemsBatchFromList(list []*raw.TransactionItems) []byte {
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
		UserId:       int32(common_utils.ParseIntOrDefault(record[0])),
		Gender:       record[1],
		Birthdate:    record[2],
		RegisteredAt: record[3],
	}
}

func UserBatchFromList(list []*raw.User) []byte {
	data, err := proto.Marshal(&raw.UserBatch{
		Users: list,
	})
	if err != nil {
		data = []byte{}
	}
	return data
}

/* --- Menu Items Data --- */

func MenuItemFromRecord(record []string) *raw.MenuItem {
	return &raw.MenuItem{
		ItemId:        int32(common_utils.ParseIntOrDefault(record[0])),
		ItemName:      record[1],
		Category:      record[2],
		Price:         common_utils.ParseFloatOrDefault(record[3]),
		IsSeasonal:    record[4] == "True",
		AvailableFrom: record[5],
		AvailableTo:   record[6],
	}
}

func MenuItemBatchFromList(list []*raw.MenuItem) []byte {
	data, err := proto.Marshal(&raw.MenuItemBatch{
		MenuItems: list,
	})
	if err != nil {
		data = []byte{}
	}
	return data
}

/* --- Stores Data --- */

func StoreFromRecord(record []string) *raw.Store {
	return &raw.Store{
		StoreId:    int32(common_utils.ParseIntOrDefault(record[0])),
		StoreName:  record[1],
		Street:     record[2],
		PostalCode: int32(common_utils.ParseIntOrDefault(record[3])),
		City:       record[4],
		State:      record[5],
		Latitude:   common_utils.ParseFloatOrDefault(record[6]),
		Longitude:  common_utils.ParseFloatOrDefault(record[7]),
	}
}

func StoreBatchFromList(list []*raw.Store) []byte {
	data, err := proto.Marshal(&raw.StoreBatch{
		Stores: list,
	})
	if err != nil {
		data = []byte{}
	}
	return data
}
