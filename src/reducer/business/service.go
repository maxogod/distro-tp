package business

import (
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

type reducerService struct{}

// TODO: Review to insted process batces of grouped data, instead of single grouped data
func NewReducerService() ReducerService {
	return &reducerService{}
}

// This is T2_1
func (rs *reducerService) SumTotalProfitBySubtotal(groupedData *group_by.GroupTransactionItems) *reduced.TotalProfitBySubtotal {
	var result reduced.TotalProfitBySubtotal

	result.ItemId = groupedData.ItemId
	result.YearMonth = groupedData.YearMonth

	var totalProfit float64 = 0
	for _, item := range groupedData.GetTransactionItems() {
		totalProfit += item.Subtotal
	}

	result.Subtotal = totalProfit

	return &result
}

// This is T2_2
func (rs *reducerService) SumTotalSoldByQuantity(groupedData *group_by.GroupTransactionItems) *reduced.TotalSoldByQuantity {
	var result reduced.TotalSoldByQuantity

	result.ItemId = groupedData.ItemId
	result.YearMonth = groupedData.YearMonth

	var totalSold int32 = 0
	for _, item := range groupedData.GetTransactionItems() {
		totalSold += item.Quantity
	}

	result.Quantity = totalSold

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
