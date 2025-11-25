package aggregator_test

import (
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var MockTotalSumItemsReport = reduced.TotalSumItemsReport{
	TotalSumItemsBySubtotal: []*reduced.TotalSumItem{
		{
			ItemId:    "item1",
			YearMonth: "2025-07",
			Quantity:  20,
			Subtotal:  2000.0,
		},
		{
			ItemId:    "item2",
			YearMonth: "2025-07",
			Quantity:  50,
			Subtotal:  5.0,
		},
	},
	TotalSumItemsByQuantity: []*reduced.TotalSumItem{
		{
			ItemId:    "item2",
			YearMonth: "2025-07",
			Quantity:  50,
			Subtotal:  5.0,
		},
		{
			ItemId:    "item1",
			YearMonth: "2025-07",
			Quantity:  20,
			Subtotal:  2000.0,
		},
	},
}

var MockTpvOutput = reduced.TotalPaymentValueBatch{
	TotalPaymentValues: []*reduced.TotalPaymentValue{
		{
			StoreId:     "1",
			Semester:    "2024-H1",
			FinalAmount: 600.0,
		},
		{
			StoreId:     "2",
			Semester:    "2025-H1",
			FinalAmount: 100.0,
		},
	},
}

var MockUsersDupQuantitiesOutput = reduced.CountedUserTransactionBatch{
	CountedUserTransactions: []*reduced.CountedUserTransactions{
		{
			StoreId:             "1",
			UserId:              "user1",
			Birthdate:           "2000-01-01",
			TransactionQuantity: 100,
		},
		{
			StoreId:             "2",
			UserId:              "user1",
			Birthdate:           "2000-01-01",
			TransactionQuantity: 10,
		},
	},
}
