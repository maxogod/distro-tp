package aggregator_test

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var MockTransactionsBatch = raw.TransactionBatch{
	Transactions: []*raw.Transaction{
		{
			TransactionId: "1", // Good
			StoreId:       "storeID",
			UserId:        "userID",
			FinalAmount:   150.0,
			CreatedAt:     "2025-07-01 06:01:00",
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
	}}

var MockTPV = reduced.TotalPaymentValueBatch{
	TotalPaymentValues: []*reduced.TotalPaymentValue{
		{
			StoreId:     "1",
			Semester:    "2024-H1",
			FinalAmount: 300.0,
		},
		{
			StoreId:     "2",
			Semester:    "2025-H1",
			FinalAmount: 100.0,
		},
		{
			StoreId:     "1",
			Semester:    "2024-H1",
			FinalAmount: 300.0,
		},
	},
}

var MockUsersDupQuantities = reduced.CountedUserTransactionBatch{
	CountedUserTransactions: []*reduced.CountedUserTransactions{
		{
			StoreId:             "1",
			UserId:              "user1",
			Birthdate:           "2000-01-01",
			TransactionQuantity: 50,
		},
		{
			StoreId:             "1",
			UserId:              "user1",
			Birthdate:           "2000-01-01",
			TransactionQuantity: 50,
		},
		{
			StoreId:             "2",
			UserId:              "user1",
			Birthdate:           "2000-01-01",
			TransactionQuantity: 10,
		},
	},
}
