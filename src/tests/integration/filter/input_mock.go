package filter_test

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

var mockTransactions = []*raw.Transaction{
	{
		TransactionId: "1", // Good
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   150.0,
		CreatedAt:     "2025-07-01 06:01:00",
	},
	{
		TransactionId: "2", // Too late
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   150.0,
		CreatedAt:     "2024-05-15 23:01:00",
	},
	{
		TransactionId: "3", // Too early
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   200.0,
		CreatedAt:     "2024-07-01 05:59:00",
	},
	{
		TransactionId: "4", // Too low amount
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   50.0,
		CreatedAt:     "2024-05-15 15:20:00",
	},
	{
		TransactionId: "5", // Too old
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   200.0,
		CreatedAt:     "2023-03-10 10:15:00",
	},
	{
		TransactionId: "6", // Good
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   75.0,
		CreatedAt:     "2025-07-01 06:00:00",
	},
	{
		TransactionId: "7", // Good
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 23:00:00",
	},
}

var MockTransactionsBatch = raw.TransactionBatch{
	Transactions: mockTransactions,
}
