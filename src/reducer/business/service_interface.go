package business

import (
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

type ReducerService interface {
	SumTotalProfitBySubtotal(groupedData *group_by.GroupTransactionItems) *reduced.TotalProfitBySubtotal
	SumTotalSoldByQuantity(groupedData *group_by.GroupTransactionItems) *reduced.TotalSoldByQuantity
	SumTotalPaymentValue(groupedData *group_by.GroupTransactions) *reduced.TotalPaymentValue
	CountUserTransactions(groupedData *group_by.GroupTransactions) *reduced.CountedUserTransactions
}
