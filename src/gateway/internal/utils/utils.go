package utils

import (
	"strconv"

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

func TransactionToCsv(record *raw.Transaction) string {
	return record.TransactionId + "," +
		strconv.Itoa(int(record.StoreId)) + "," +
		strconv.Itoa(int(record.PaymentMethod)) + "," +
		strconv.Itoa(int(record.VoucherId)) + "," +
		strconv.Itoa(int(record.UserId)) + "," +
		strconv.Itoa(int(record.OriginalAmount)) + "," +
		strconv.Itoa(int(record.DiscountApplied)) + "," +
		strconv.Itoa(int(record.FinalAmount)) + "," +
		record.CreatedAt + "\n"
}

/* --- Transaction Items Data --- */

func TransactionItemsFromRecord(record []string) *raw.TransactionItems {
	return &raw.TransactionItems{
		TransactionId: record[0],
		ItemId:        int64(common_utils.ParseIntOrDefault(record[1])),
		Quantity:      int32(common_utils.ParseIntOrDefault(record[2])),
		UnitPrice:     common_utils.ParseFloatOrDefault(record[3]),
		Subtotal:      common_utils.ParseFloatOrDefault(record[4]),
		CreatedAt:     record[8],
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

func TransactionItemsToCsv(record *raw.TransactionItems) string {
	return record.TransactionId + "," +
		strconv.Itoa(int(record.ItemId)) + "," +
		strconv.Itoa(int(record.Quantity)) + "," +
		strconv.Itoa(int(record.UnitPrice)) + "," +
		strconv.Itoa(int(record.Subtotal)) +
		record.CreatedAt + "\n"
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

func UserToCsv(record *raw.User) string {
	return strconv.Itoa(int(record.UserId)) + "," +
		record.Gender + "," +
		record.Birthdate + "," +
		record.RegisteredAt + "\n"
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

func MenuItemToCsv(record *raw.MenuItem) string {
	isSeasonal := "False"
	if record.IsSeasonal {
		isSeasonal = "True"
	}
	return strconv.Itoa(int(record.ItemId)) + "," +
		record.ItemName + "," +
		record.Category + "," +
		strconv.Itoa(int(record.Price)) + "," +
		isSeasonal + "," +
		record.AvailableFrom + "," +
		record.AvailableTo + "\n"
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

func StoreToCsv(record *raw.Store) string {
	return strconv.Itoa(int(record.StoreId)) + "," +
		record.StoreName + "," +
		record.Street + "," +
		strconv.Itoa(int(record.PostalCode)) + "," +
		record.City + "," +
		record.State + "," +
		strconv.Itoa(int(record.Latitude)) + "," +
		strconv.Itoa(int(record.Longitude)) + "\n"
}
