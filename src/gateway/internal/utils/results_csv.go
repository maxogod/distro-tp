package utils

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

const (
	T1_RES_HEADER   = "transaction_id,final_amount\n"
	T2_1_RES_HEADER = "year_month_created_at,item_name,profit_sum\n"
	T2_2_RES_HEADER = "year_month_created_at,item_name,sellings_qty\n"
	T3_RES_HEADER   = "year_half_created_at,store_name,tpv\n"
	T4_RES_HEADER   = "store_name,birthdate,purchases_qty\n"
)

// TODO: new proto to match the appropiate name of the fields
func TransactionToCsv(record *raw.Transaction) string {
	csvStr := fmt.Sprintf("%s,%.2f\n", record.TransactionId, record.FinalAmount)
	return csvStr
}

func BestSellingItemsToCsv(record *reduced.TotalSoldByQuantity) string {
	csvStr := fmt.Sprintf("%s,%s,%d\n", record.YearMonth, record.ItemId, record.Quantity)
	return csvStr
}

func MostProfitableItemsToCsv(record *reduced.TotalProfitBySubtotal) string {
	csvStr := fmt.Sprintf("%s,%s,%.2f\n", record.YearMonth, record.ItemId, record.Subtotal)
	return csvStr
}

func TopStoresByTPVToCsv(record *reduced.TotalPaymentValue) string {
	csvStr := fmt.Sprintf("%s,%s,%.2f\n", record.Semester, record.StoreId, record.FinalAmount)
	return csvStr
}

func TopUsersByPurchasesToCsv(record *reduced.CountedUserTransactions) string {
	csvStr := fmt.Sprintf("%s,%s,%d\n", record.StoreId, record.Birthdate, record.TransactionQuantity)
	return csvStr
}
