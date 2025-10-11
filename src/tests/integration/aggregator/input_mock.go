package filter_test

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

var MockTotalProfit = []*reduced.TotalProfitBySubtotal{
	{
		ItemId:    "item1",
		YearMonth: "2025-07",
		Subtotal:  100.0,
	},
	{
		ItemId:    "item1",
		YearMonth: "2025-07", // TOP 1 2025-07
		Subtotal:  100.0,
	},
	{
		ItemId:    "item1",
		YearMonth: "2024-07",
		Subtotal:  150.0,
	},
	{
		ItemId:    "item2",
		YearMonth: "2025-07",
		Subtotal:  199.0,
	},
	{
		ItemId:    "item3",
		YearMonth: "2024-07", // TOP 1 2024-07
		Subtotal:  199.0,
	},
}

var MockTotalSales = []*reduced.TotalSoldByQuantity{
	{
		ItemId:    "item1",
		YearMonth: "2025-07",
		Quantity:  100.0,
	},
	{
		ItemId:    "item1",
		YearMonth: "2025-07", // TOP 1 2025-07
		Quantity:  100.0,
	},
	{
		ItemId:    "item1",
		YearMonth: "2024-07",
		Quantity:  150.0,
	},
	{
		ItemId:    "item2",
		YearMonth: "2025-07",
		Quantity:  199.0,
	},
	{
		ItemId:    "item3",
		YearMonth: "2024-07", // TOP 1 2024-07
		Quantity:  199.0,
	},
}

var MockTPV = []*reduced.TotalPaymentValue{
	{
		StoreId:     "1",
		Semester:    "2025-H2",
		FinalAmount: 100.0,
	},
	{
		StoreId:     "1",
		Semester:    "2025-H2",
		FinalAmount: 100.0,
	},
	{
		StoreId:     "2",
		Semester:    "2025-H1",
		FinalAmount: 50.0,
	},
	{
		StoreId:     "2",
		Semester:    "2025-H1",
		FinalAmount: 50.0,
	},
	{
		StoreId:     "3",
		Semester:    "2025-H1",
		FinalAmount: 10.0,
	},
}

var MockUsers = []*reduced.CountedUserTransactions{
	{
		StoreId:             "1",
		UserId:              "user1",
		Birthdate:           "2000-01-01",
		TransactionQuantity: 100,
	},
	{
		StoreId:             "1",
		UserId:              "user1",
		Birthdate:           "2000-01-01", // TOP 1 user1@store1
		TransactionQuantity: 100,
	},
	{
		StoreId:             "2",
		UserId:              "user1",
		Birthdate:           "2000-02-02",
		TransactionQuantity: 100,
	},
	{
		StoreId:             "1",
		UserId:              "user2",
		Birthdate:           "2000-02-02",
		TransactionQuantity: 199,
	},
	{
		StoreId:             "2",
		UserId:              "user3",
		Birthdate:           "2000-02-02", // TOP 1 user3@store2
		TransactionQuantity: 199,
	},
}
