package integration

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

// if T3: the output should be grouped by StoreId and Semester (H1 or H2)
// if T4: the output should be grouped by StoreID and UserId
var mockTransactionsInput = []*raw.Transaction{
	{
		TransactionId: "tx1",
		StoreId:       "store1",
		UserId:        "user1",
		FinalAmount:   150.0,
		CreatedAt:     "2025-07-01 07:00:00",
	},
	{
		TransactionId: "tx2",
		StoreId:       "store1",
		UserId:        "user2",
		FinalAmount:   50.0,
		CreatedAt:     "2024-07-15 08:20:00",
	},
	{
		TransactionId: "tx3",
		StoreId:       "store2",
		UserId:        "user2",
		FinalAmount:   50.0,
		CreatedAt:     "2024-01-15 15:20:00",
	},
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
}

var MockTransactionsBatch = raw.TransactionBatch{
	Transactions: mockTransactionsInput,
}

// if T2: the output should be grouped by ItemId and YearMonth
var mockTransactionsItemsInput = []*raw.TransactionItem{
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
	{
		ItemId:    "item1",
		Quantity:  2,
		Subtotal:  200.0,
		CreatedAt: "2025-08-01 07:00:00",
	},
	{
		ItemId:    "item3",
		Quantity:  2,
		Subtotal:  200.0,
		CreatedAt: "2025-07-01 07:00:00",
	},
}

var MockTransactionsItemsBatch = raw.TransactionItemsBatch{
	TransactionItems: mockTransactionsItemsInput,
}
