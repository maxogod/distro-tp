package joiner_test

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var MockStores = []*raw.Store{
	{
		StoreId:   "1",
		StoreName: "Starbucks",
	},
	{
		StoreId:   "2",
		StoreName: "Dunkin' Donuts",
	},
	{
		StoreId:   "3",
		StoreName: "Hijos del Mar",
	},
}

var MockUsers = []*raw.User{
	{
		UserId:    "1",
		Birthdate: "2000-08-10",
	},
	{
		UserId:    "2",
		Birthdate: "2004-07-11",
	},
	{
		UserId:    "3",
		Birthdate: "1905-01-01",
	},
}

var MockTPV = reduced.TotalPaymentValueBatch{
	TotalPaymentValues: []*reduced.TotalPaymentValue{
		{
			StoreId:     "1",
			Semester:    "2024-H1",
			FinalAmount: 100.0,
		},
		{
			StoreId:     "2",
			Semester:    "2025-H1",
			FinalAmount: 100.0,
		},
		{
			StoreId:     "1",
			Semester:    "2024-H2",
			FinalAmount: 100.0,
		},
		{
			StoreId:     "3",
			Semester:    "2024-H1",
			FinalAmount: 100.0,
		},
		{
			StoreId:     "3",
			Semester:    "2024-H2",
			FinalAmount: 100.0,
		},
	},
}

var MockCountedUserTransactions = reduced.CountedUserTransactionBatch{
	CountedUserTransactions: []*reduced.CountedUserTransactions{
		{
			StoreId:             "1",
			UserId:              "1",
			TransactionQuantity: 50,
		},
		{
			StoreId:             "2",
			UserId:              "2",
			TransactionQuantity: 50,
		},
		{
			StoreId:             "3",
			UserId:              "3",
			TransactionQuantity: 50,
		},
	},
}
