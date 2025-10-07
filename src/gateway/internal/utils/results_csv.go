package utils

import (
	"strconv"

	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

const (
	T1_RES_HEADER   = "transaction_id,store_id,user_id,final_amount,created_at\n"
	T2_1_RES_HEADER = "year_month_created,item_name,profit_sum\n"
	T2_2_RES_HEADER = "year_month_created,item_name,sellings_qty\n"
	T3_RES_HEADER   = "year_half_created_at,store_name,tpv\n"
	T4_RES_HEADER   = "store_name,user_birthdate,purchases_qty\n"
)

// TODO: new proto to match the appropiate name of the fields
func TransactionToCsv(record *raw.Transaction) string {
	return record.TransactionId + "," +
		record.StoreId + "," +
		record.UserId + "," +
		strconv.Itoa(int(record.FinalAmount)) + "," +
		record.CreatedAt + "\n"
}

func BestSellingItemsToCsv(record *reduced.TotalSoldByQuantity) string {
	return record.YearMonth + "," +
		record.ItemId + "," +
		strconv.Itoa(int(record.Quantity)) + "\n"
}

func MostProfitableItemsToCsv(record *reduced.TotalProfitBySubtotal) string {
	return record.YearMonth + "," +
		record.ItemId + "," +
		strconv.Itoa(int(record.Subtotal)) + "\n"
}

func TopStoresByTPVToCsv(record *reduced.TotalPaymentValue) string {
	return record.Semester + "," +
		record.StoreId + "," +
		strconv.Itoa(int(record.FinalAmount)) + "\n"
}

func TopUsersByPurchasesToCsv(record *reduced.CountedUserTransactions) string {
	return record.StoreId + "," +
		record.UserId + "," +
		strconv.Itoa(int(record.TransactionQuantity)) + "\n"
}
