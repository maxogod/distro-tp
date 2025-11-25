package joiner_test

import "github.com/maxogod/distro-tp/src/common/models/reduced"

// var MockTotalProfitOutput = []*reduced.TotalProfitBySubtotal{
// 	{
// 		ItemId:    "Black Coffee",
// 		YearMonth: "2025-01",
// 		Subtotal:  100.0,
// 	},
// 	{
// 		ItemId:    "Black Coffee",
// 		YearMonth: "2025-01",
// 		Subtotal:  100.0,
// 	},
// 	{
// 		ItemId:    "Latte",
// 		YearMonth: "2024-12",
// 		Subtotal:  100.0,
// 	},
// 	{
// 		ItemId:    "Flat White",
// 		YearMonth: "2024-06",
// 		Subtotal:  100.0,
// 	},
// }

// var MockTotalSalesOutput = []*reduced.TotalSoldByQuantity{
// 	{
// 		ItemId:    "Black Coffee",
// 		YearMonth: "2025-01",
// 		Quantity:  100.0,
// 	},
// 	{
// 		ItemId:    "Black Coffee",
// 		YearMonth: "2025-01",
// 		Quantity:  100.0,
// 	},
// 	{
// 		ItemId:    "Latte",
// 		YearMonth: "2024-12",
// 		Quantity:  100.0,
// 	},
// 	{
// 		ItemId:    "Flat White",
// 		YearMonth: "2024-06",
// 		Quantity:  100.0,
// 	},
// }

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
