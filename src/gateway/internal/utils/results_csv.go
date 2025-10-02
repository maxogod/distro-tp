package utils

import (
	"strconv"

	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

const (
	T1_RES_HEADER   = "transaction_id,store_id,payment_method,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at\n"
	T2_1_RES_HEADER = "year_month_created,item_name,profit_sum\n"
	T2_2_RES_HEADER = "year_month_created,item_name,sellings_qty\n"
	T3_RES_HEADER   = "year_half_created_at,store_name,tpv\n"
	T4_RES_HEADER   = "store_name,user_birthdate,purchases_qty\n"
)

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

func BestSellingItemsToCsv(record *joined.JoinBestSellingProducts) string {
	return record.YearMonthCreatedAt + "," +
		record.ItemName + "," +
		strconv.Itoa(int(record.SellingsQty)) + "\n"
}

func MostProfitableItemsToCsv(record *joined.JoinMostProfitsProducts) string {
	return record.YearMonthCreatedAt + "," +
		record.ItemName + "," +
		strconv.Itoa(int(record.ProfitSum)) + "\n"
}

func TopStoresByTPVToCsv(record *joined.JoinStoreTPV) string {
	return record.YearHalfCreatedAt + "," +
		record.StoreName + "," +
		strconv.Itoa(int(record.Tpv)) + "\n"
}

func TopUsersByPurchasesToCsv(record *joined.JoinMostPurchasesUser) string {
	return record.StoreName + "," +
		record.UserBirthdate + "," +
		strconv.Itoa(int(record.PurchasesQty)) + "\n"
}
