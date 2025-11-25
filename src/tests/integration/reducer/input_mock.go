package reducer_test

import (
	"github.com/maxogod/distro-tp/src/common/models/group_by"
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

// if T3: the output should be grouped by StoreId and Semester (H1 or H2)
var GroupTransactionMock1 = group_by.GroupTransactionsBatch{
	GroupedTransactions: []*group_by.GroupTransactions{
		{
			StoreId:  "store1",
			Semester: "2025-H2",
			Transactions: []*raw.Transaction{
				{
					TransactionId: "tx1",
					StoreId:       "store1",
					UserId:        "user1",
					FinalAmount:   150.0,
					CreatedAt:     "2025-07-01 07:00:00",
				},
				{
					TransactionId: "tx2",
					StoreId:       "store1",
					UserId:        "user2",
					FinalAmount:   50.0,
					CreatedAt:     "2024-07-15 08:20:00",
				},
			},
		},
	},
}

var GroupTransactionMock4 = group_by.GroupTransactionsBatch{
	GroupedTransactions: []*group_by.GroupTransactions{
		{
			StoreId: "store3",
			UserId:  "user1",
			Transactions: []*raw.Transaction{
				{
					TransactionId: "tx4",
					StoreId:       "store3",
					UserId:        "user1",
					FinalAmount:   200.0,
					CreatedAt:     "2021-01-01 07:00:00",
				},
				{
					TransactionId: "tx5",
					StoreId:       "store3",
					UserId:        "user1",
					FinalAmount:   200.0,
					CreatedAt:     "2021-03-10 10:15:00",
				},
			},
		},
	},
}
