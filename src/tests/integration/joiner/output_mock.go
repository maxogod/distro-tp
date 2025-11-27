package joiner_test

import "github.com/maxogod/distro-tp/src/common/models/reduced"

var MockTpvOutput = reduced.TotalPaymentValueBatch{
	TotalPaymentValues: []*reduced.TotalPaymentValue{
		{
			StoreId:     "Starbucks",
			Semester:    "2024-H1",
			FinalAmount: 100.0,
		},
		{
			StoreId:     "Dunkin' Donuts",
			Semester:    "2025-H1",
			FinalAmount: 100.0,
		},
		{
			StoreId:     "Starbucks",
			Semester:    "2024-H2",
			FinalAmount: 100.0,
		},
		{
			StoreId:     "Hijos del Mar",
			Semester:    "2024-H1",
			FinalAmount: 100.0,
		},
		{
			StoreId:     "Hijos del Mar",
			Semester:    "2024-H2",
			FinalAmount: 100.0,
		},
	},
}

var MockCountedUserTransactionsOutput = reduced.CountedUserTransactionBatch{
	CountedUserTransactions: []*reduced.CountedUserTransactions{
		{
			StoreId:             "Starbucks",
			UserId:              "1",
			Birthdate:           "2000-08-10",
			TransactionQuantity: 50,
		},
		{
			StoreId:             "Dunkin' Donuts",
			UserId:              "2",
			Birthdate:           "2004-07-11",
			TransactionQuantity: 50,
		},
		{
			StoreId:             "Hijos del Mar",
			UserId:              "3",
			Birthdate:           "1905-01-01",
			TransactionQuantity: 50,
		},
	},
}
