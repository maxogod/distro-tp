package integration

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

var mockTransactions = []*raw.Transaction{
	{
		TransactionId: "tx1",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   150.0,
		CreatedAt:     "2025-07-01 07:00:00",
	},
	{
		TransactionId: "tx2",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   50.0,
		CreatedAt:     "2024-05-15 08:20:00",
	},
	{
		TransactionId: "tx3",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   50.0,
		CreatedAt:     "2024-05-15 15:20:00",
	},
	{
		TransactionId: "tx4",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   200.0,
		CreatedAt:     "2021-07-01 07:00:00",
	},
	{
		TransactionId: "tx5",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   200.0,
		CreatedAt:     "2021-03-10 10:15:00",
	},
}

var MockTransactionsBatch = raw.TransactionBatch{
	Transactions: mockTransactions,
}
