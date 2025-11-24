package business

import (
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

type groupService struct{}

func NewGroupService() GroupService {
	return &groupService{}
}

// This is T2
func (gs *groupService) GroupItemsByYearMonthAndItem(items []*raw.TransactionItem) map[string]*group_by.GroupTransactionItems {
	result := make(map[string]*group_by.GroupTransactionItems)

	for _, item := range items {
		// Since the CreatedAt is in the format "YYYY-MM-DD HH:MM:SS",
		// we extract the "YYYY-MM" part and replace the CreatedAt field with it
		item.CreatedAt = item.CreatedAt[:7]

		key := item.CreatedAt + "@" + item.ItemId
		if _, exists := result[key]; !exists {
			result[key] = &group_by.GroupTransactionItems{
				ItemId:           item.ItemId,
				YearMonth:        item.CreatedAt,
				TransactionItems: []*raw.TransactionItem{item},
			}
		} else {
			result[key].TransactionItems = append(result[key].GetTransactionItems(), item)
		}
	}

	return result

}

// This is T3
func (gs *groupService) GroupTransactionsByStoreAndSemester(transactions []*raw.Transaction) map[string]*group_by.GroupTransactions {
	result := make(map[string]*group_by.GroupTransactions)

	for _, transaction := range transactions {

		// Since the CreatedAt is in the format "YYYY-MM-DD HH:MM:SS",
		// we extract the "YYYY-MM" part to determine the semester
		// and replace the CreatedAt field with it
		var semester string
		month := transaction.CreatedAt[5:7]
		year := transaction.CreatedAt[:4]

		if month >= "01" && month <= "06" {
			semester = "H1"
		} else {
			semester = "H2"
		}

		transaction.CreatedAt = year + "-" + semester

		key := transaction.CreatedAt + "@" + transaction.StoreId
		if _, exists := result[key]; !exists {
			result[key] = &group_by.GroupTransactions{
				StoreId:      transaction.StoreId,
				Semester:     transaction.CreatedAt,
				Transactions: []*raw.Transaction{transaction},
			}
		} else {
			result[key].Transactions = append(result[key].GetTransactions(), transaction)
		}
	}

	return result
}

// This is T4
func (gs *groupService) GroupTransactionsByStoreAndUser(transactions []*raw.Transaction) map[string]*group_by.GroupTransactions {
	result := make(map[string]*group_by.GroupTransactions)

	for _, transaction := range transactions {
		key := transaction.UserId + "@" + transaction.StoreId
		if _, exists := result[key]; !exists {
			result[key] = &group_by.GroupTransactions{
				StoreId:      transaction.StoreId,
				UserId:       transaction.UserId,
				Transactions: []*raw.Transaction{transaction},
			}
		} else {
			result[key].Transactions = append(result[key].GetTransactions(), transaction)
		}
	}

	return result
}
