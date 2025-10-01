package mock

import (
	"time"

	"github.com/maxogod/distro-tp/src/common/models/raw"
)

var MockTransactions = []*raw.Transaction{
	{
		TransactionId:   "tx1",
		StoreId:         101,
		PaymentMethod:   1,
		VoucherId:       1001,
		UserId:          501,
		OriginalAmount:  200.0,
		DiscountApplied: 50.0,
		FinalAmount:     150.0,
		CreatedAt:       time.Date(2025, 11, 8, 10, 30, 0, 0, time.UTC).Format("2025-09-28 18:11:05"),
	},
	{
		TransactionId:   "tx2",
		StoreId:         102,
		PaymentMethod:   2,
		VoucherId:       1002,
		UserId:          502,
		OriginalAmount:  100.0,
		DiscountApplied: 25.0,
		FinalAmount:     50.0,
		CreatedAt:       time.Date(2024, 5, 15, 8, 20, 0, 0, time.UTC).Format("2025-09-28 18:11:05"),
	},
	{
		TransactionId:   "tx3",
		StoreId:         102,
		PaymentMethod:   2,
		VoucherId:       1002,
		UserId:          502,
		OriginalAmount:  100.0,
		DiscountApplied: 25.0,
		FinalAmount:     50.0,
		CreatedAt:       time.Date(2024, 5, 15, 15, 20, 0, 0, time.UTC).Format("2025-09-28 18:11:05"),
	},
	{
		TransactionId:   "tx4",
		StoreId:         103,
		PaymentMethod:   1,
		VoucherId:       1003,
		UserId:          503,
		OriginalAmount:  250.0,
		DiscountApplied: 50.0,
		FinalAmount:     200.0,
		CreatedAt:       time.Date(2022, 3, 10, 10, 15, 0, 0, time.UTC).Format("2025-09-28 18:11:05"),
	},
	{
		TransactionId:   "tx5",
		StoreId:         103,
		PaymentMethod:   1,
		VoucherId:       1003,
		UserId:          503,
		OriginalAmount:  250.0,
		DiscountApplied: 50.0,
		FinalAmount:     200.0,
		CreatedAt:       time.Date(2021, 3, 10, 10, 15, 0, 0, time.UTC).Format("2025-09-28 18:11:05"),
	},
}

var MockTransactionItems = []*raw.TransactionItems{
	{
		TransactionId: "tx1",
		ItemId:        1,
		Quantity:      2,
		UnitPrice:     30.0,
		Subtotal:      60.0,
		CreatedAt:     time.Date(2025, 11, 8, 11, 15, 0, 0, time.UTC).Format("2025-09-28 18:11:05"),
	},
	{
		TransactionId: "tx2",
		ItemId:        2,
		Quantity:      1,
		UnitPrice:     25.0,
		Subtotal:      25.0,
		CreatedAt:     time.Date(2024, 6, 10, 16, 45, 0, 0, time.UTC).Format("2025-09-28 18:11:05"),
	},
	{
		TransactionId: "tx3",
		ItemId:        3,
		Quantity:      3,
		UnitPrice:     50.0,
		Subtotal:      150.0,
		CreatedAt:     time.Date(2023, 3, 10, 9, 0, 0, 0, time.UTC).Format("2025-09-28 18:11:05"),
	},
	{
		TransactionId: "tx4",
		ItemId:        4,
		Quantity:      1,
		UnitPrice:     90.0,
		Subtotal:      90.0,
		CreatedAt:     time.Date(2022, 11, 8, 11, 20, 0, 0, time.UTC).Format("2025-09-28 18:11:05"),
	},
}
