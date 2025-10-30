package utils

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
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
