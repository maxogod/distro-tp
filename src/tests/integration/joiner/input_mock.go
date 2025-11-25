package joiner_test

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var MockMenuItems = []*raw.MenuItem{
	{
		ItemId:   "item1",
		ItemName: "Black Coffee",
	},
	{
		ItemId:   "item2",
		ItemName: "Latte",
	},
	{
		ItemId:   "item3",
		ItemName: "Flat White",
	},
}

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

// var MockTotalProfit = []*reduced.TotalProfitBySubtotal{
// 	{
// 		ItemId:    "item1",
// 		YearMonth: "2025-01",
// 		Subtotal:  100.0,
// 	},
// 	{
// 		ItemId:    "item1",
// 		YearMonth: "2025-01",
// 		Subtotal:  100.0,
// 	},
// 	{
// 		ItemId:    "item2",
// 		YearMonth: "2024-12",
// 		Subtotal:  100.0,
// 	},
// 	{
// 		ItemId:    "item3",
// 		YearMonth: "2024-06",
// 		Subtotal:  100.0,
// 	},
// }

// var MockTotalSales = []*reduced.TotalSoldByQuantity{
// 	{
// 		ItemId:    "item1",
// 		YearMonth: "2025-01",
// 		Quantity:  100.0,
// 	},
// 	{
// 		ItemId:    "item1",
// 		YearMonth: "2025-01",
// 		Quantity:  100.0,
// 	},
// 	{
// 		ItemId:    "item2",
// 		YearMonth: "2024-12",
// 		Quantity:  100.0,
// 	},
// 	{
// 		ItemId:    "item3",
// 		YearMonth: "2024-06",
// 		Quantity:  100.0,
// 	},
// }

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
