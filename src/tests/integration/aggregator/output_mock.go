package filter_test

import (
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var MockTpvOutput = []*reduced.TotalPaymentValue{
	{
		StoreId:     "1",
		Semester:    "2024-H1",
		FinalAmount: 200.0,
	},
	{
		StoreId:     "1",
		Semester:    "2024-H2",
		FinalAmount: 200.0,
	},
	{
		StoreId:     "2",
		Semester:    "2025-H1",
		FinalAmount: 100.0,
	},
}

// var MockUsersDupQuantitiesOutput = []*reduced.CountedUserTransactions{
// 	{
// 		StoreId:             "1",
// 		UserId:              "user3",
// 		Birthdate:           "2000-01-01",
// 		TransactionQuantity: 200,
// 	},
// 	{
// 		StoreId:             "1",
// 		UserId:              "user1",
// 		Birthdate:           "2000-01-01",
// 		TransactionQuantity: 100,
// 	},
// 	{
// 		StoreId:             "1",
// 		UserId:              "user2",
// 		Birthdate:           "2000-01-01",
// 		TransactionQuantity: 100,
// 	},
// 	{
// 		StoreId:             "1",
// 		UserId:              "user4",
// 		Birthdate:           "2000-01-01",
// 		TransactionQuantity: 50,
// 	},
// 	{
// 		StoreId:             "2",
// 		UserId:              "user1",
// 		Birthdate:           "2000-01-01",
// 		TransactionQuantity: 10,
// 	},
// }

var MockUsersDupQuantitiesOutput = map[string](map[int32]int){
	"1": {
		200: 1,
		100: 2,
		50:  1,
	},
	"2": {
		10: 1,
	},
}

var MockTotalProfitOutput = []*reduced.TotalProfitBySubtotal{
	{
		ItemId:    "item1",
		YearMonth: "2024-06",
		Subtotal:  100.0,
	},
	{
		ItemId:    "item2",
		YearMonth: "2024-12",
		Subtotal:  100.0,
	},
	{
		ItemId:    "item1",
		YearMonth: "2025-01",
		Subtotal:  200.0,
	},
}

var MockTotalQuantityOutput = []*reduced.TotalSoldByQuantity{
	{
		ItemId:    "item1",
		YearMonth: "2024-06",
		Quantity:  100.0,
	},
	{
		ItemId:    "item2",
		YearMonth: "2024-12",
		Quantity:  100.0,
	},
	{
		ItemId:    "item1",
		YearMonth: "2025-01",
		Quantity:  200.0,
	},
}
