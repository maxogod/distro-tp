package aggregator_test

import (
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var MockTotalSumItems = reduced.TotalSumItemsBatch{
	TotalSumItems: []*reduced.TotalSumItem{
		{
			ItemId:    "item1",
			YearMonth: "2025-07",
			Quantity:  10,
			Subtotal:  1000.0,
		},
		{
			ItemId:    "item1",
			YearMonth: "2025-07",
			Quantity:  10,
			Subtotal:  1000.0,
		},
		{
			ItemId:    "item2",
			YearMonth: "2025-08",
			Quantity:  50,
			Subtotal:  5.0,
		},
	},
}

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
