package group_by_test

import "github.com/maxogod/distro-tp/src/common/models/raw"

var MockTransactionsOutputT3 = map[string][]*raw.Transaction{
	"store1@2025-H2": {
		{
			TransactionId: "tx1",
			StoreId:       "store1",
			UserId:        "user1",
			FinalAmount:   150.0,
			CreatedAt:     "2025-07-01 07:00:00",
		},
	},
	"store1@2024-H2": {
		{
			TransactionId: "tx2",
			StoreId:       "store1",
			UserId:        "user2",
			FinalAmount:   50.0,
			CreatedAt:     "2024-07-15 08:20:00",
		},
	},
	"store2@2024-H1": {
		{
			TransactionId: "tx3",
			StoreId:       "store2",
			UserId:        "user2",
			FinalAmount:   50.0,
			CreatedAt:     "2024-01-15 15:20:00",
		},
	},
	"store3@2021-H1": {
		{
			TransactionId: "tx4",
			StoreId:       "store3",
			UserId:        "user1",
			FinalAmount:   200.0,
			CreatedAt:     "2021-01-01 07:00:00",
		},
		{
			TransactionId: "tx5",
			StoreId:       "store3",
			UserId:        "user1",
			FinalAmount:   200.0,
			CreatedAt:     "2021-03-10 10:15:00",
		},
	},
}

var MockTransactionsOutputT4 = map[string][]*raw.Transaction{
	"store1@user1": {
		{
			TransactionId: "tx1",
			StoreId:       "store1",
			UserId:        "user1",
			FinalAmount:   150.0,
			CreatedAt:     "2025-07-01 07:00:00",
		},
	},
	"store1@user2": {
		{
			TransactionId: "tx2",
			StoreId:       "store1",
			UserId:        "user2",
			FinalAmount:   50.0,
			CreatedAt:     "2024-07-15 08:20:00",
		},
	},
	"store2@user2": {
		{
			TransactionId: "tx3",
			StoreId:       "store2",
			UserId:        "user2",
			FinalAmount:   50.0,
			CreatedAt:     "2024-01-15 15:20:00",
		},
	},
	"store3@user1": {
		{
			TransactionId: "tx4",
			StoreId:       "store3",
			UserId:        "user1",
			FinalAmount:   200.0,
			CreatedAt:     "2021-01-01 07:00:00",
		},
		{
			TransactionId: "tx5",
			StoreId:       "store3",
			UserId:        "user1",
			FinalAmount:   200.0,
			CreatedAt:     "2021-03-10 10:15:00",
		},
	},
}

var MockItemsOutputT2 = map[string][]*raw.TransactionItem{
	"item1@2025-07": {
		{
			ItemId:    "item1",
			Quantity:  2,
			Subtotal:  200.0,
			CreatedAt: "2025-07-01 07:00:00",
		},
		{
			ItemId:    "item1",
			Quantity:  2,
			Subtotal:  200.0,
			CreatedAt: "2025-07-01 07:00:00",
		},
	},
	"item1@2025-08": {

		{
			ItemId:    "item1",
			Quantity:  2,
			Subtotal:  200.0,
			CreatedAt: "2025-08-01 07:00:00",
		},
	},
	"item3@2025-07": {

		{
			ItemId:    "item3",
			Quantity:  2,
			Subtotal:  200.0,
			CreatedAt: "2025-07-01 07:00:00",
		},
	},
}
