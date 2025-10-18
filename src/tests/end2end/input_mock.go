package eof

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

var MockTransactionsBatchT1 = raw.TransactionBatch{
	Transactions: []*raw.Transaction{
		{
			TransactionId: "1", // Good
			StoreId:       "storeID",
			UserId:        "userID",
			FinalAmount:   150.0,
			CreatedAt:     "2025-07-01 06:01:00",
		},
		{
			TransactionId: "2", // Good
			StoreId:       "storeID",
			UserId:        "userID",
			FinalAmount:   75.0,
			CreatedAt:     "2025-07-01 06:00:00",
		},
		{
			TransactionId: "3", // Good
			StoreId:       "storeID",
			UserId:        "userID",
			FinalAmount:   100.0,
			CreatedAt:     "2025-07-01 23:00:00",
		},
		{
			TransactionId: "4", // will be filtered
			StoreId:       "storeID",
			UserId:        "userID",
			FinalAmount:   10.0,
			CreatedAt:     "2025-07-01 06:01:00",
		},
		{
			TransactionId: "5", // will be filtered
			StoreId:       "storeID",
			UserId:        "userID",
			FinalAmount:   7.0,
			CreatedAt:     "2025-07-01 06:00:00",
		},
		{
			TransactionId: "6", // will be filtered
			StoreId:       "storeID",
			UserId:        "userID",
			FinalAmount:   10.0,
			CreatedAt:     "2025-07-01 23:00:00",
		},
	}}

var MockTransactionsBatchT3 = raw.TransactionBatch{
	Transactions: []*raw.Transaction{
		{
			TransactionId: "1",
			StoreId:       "store1",
			UserId:        "userID",
			FinalAmount:   100.0,
			CreatedAt:     "2025-02-01 08:01:00",
		},
		{
			TransactionId: "2",
			StoreId:       "store1",
			UserId:        "userID",
			FinalAmount:   200.0,
			CreatedAt:     "2025-02-01 08:00:00",
		},
		{
			TransactionId: "3",
			StoreId:       "store1",
			UserId:        "userID",
			FinalAmount:   300.0,
			CreatedAt:     "2025-02-01 08:00:00",
		},
		{
			TransactionId: "1",
			StoreId:       "store2",
			UserId:        "userID",
			FinalAmount:   100.0,
			CreatedAt:     "2024-02-01 08:01:00",
		},
		{
			TransactionId: "2",
			StoreId:       "store2",
			UserId:        "userID",
			FinalAmount:   200.0,
			CreatedAt:     "2024-02-01 08:00:00",
		},
		{
			TransactionId: "3",
			StoreId:       "store2",
			UserId:        "userID",
			FinalAmount:   300.0,
			CreatedAt:     "2024-02-01 08:00:00",
		},
	}}

var MockStoreRefData = raw.StoreBatch{
	Stores: []*raw.Store{
		{
			StoreId:   "store1",
			StoreName: "Store One",
		},
		{
			StoreId:   "store2",
			StoreName: "Store Two",
		},
	},
}
