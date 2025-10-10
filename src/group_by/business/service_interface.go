package business

import (
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

type GroupService interface {
	GroupItemsByYearMonthAndItem(items []*raw.TransactionItem) map[string]*group_by.GroupTransactionItems
	GroupTransactionsByStoreAndSemester(transactions []*raw.Transaction) map[string]*group_by.GroupTransactions
	GroupTransactionsByStoreAndUser(transactions []*raw.Transaction) map[string]*group_by.GroupTransactions
}
