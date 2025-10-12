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

var MockUsersOutput = []*reduced.CountedUserTransactions{
	{
		StoreId:             "2",
		UserId:              "user3",
		Birthdate:           "2000-02-02",
		TransactionQuantity: 60,
	},
	{
		StoreId:             "1",
		UserId:              "user2",
		Birthdate:           "2000-01-01",
		TransactionQuantity: 40,
	},
	{
		StoreId:             "1",
		UserId:              "user1",
		Birthdate:           "2000-01-01",
		TransactionQuantity: 20,
	},
}
