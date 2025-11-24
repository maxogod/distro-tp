package business

import (
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

type reducerService struct{}

func NewReducerService() ReducerService {
	return &reducerService{}
}

// This is T2
func (rs *reducerService) SumTotalItems(groupedData *group_by.GroupTransactionItems) *reduced.TotalSumItem {
	var result reduced.TotalSumItem

	result.ItemId = groupedData.ItemId
	result.YearMonth = groupedData.YearMonth

	var totalSold int32 = 0
	var totalProfit float64 = 0
	for _, item := range groupedData.GetTransactionItems() {
		totalSold += item.Quantity
		totalProfit += item.Subtotal
	}

	result.Quantity = totalSold
	result.Subtotal = totalProfit

	return &result

}

// This is T3
func (rs *reducerService) SumTotalPaymentValue(groupedData *group_by.GroupTransactions) *reduced.TotalPaymentValue {
	var result reduced.TotalPaymentValue

	result.StoreId = groupedData.GetStoreId()
	result.Semester = groupedData.GetSemester()
	var finalAmount float64 = 0
	for _, item := range groupedData.GetTransactions() {
		finalAmount += item.FinalAmount
	}
	result.FinalAmount = finalAmount

	return &result
}

// This is T4
func (rs *reducerService) CountUserTransactions(groupedData *group_by.GroupTransactions) *reduced.CountedUserTransactions {
	var result reduced.CountedUserTransactions

	result.UserId = groupedData.GetUserId()
	result.Birthdate = groupedData.GetUserId()
	result.StoreId = groupedData.GetStoreId()
	result.TransactionQuantity = int32(len(groupedData.GetTransactions()))

	return &result
}
