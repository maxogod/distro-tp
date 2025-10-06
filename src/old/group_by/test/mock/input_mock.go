package mock

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

var MockTransactionsInput = []*raw.Transaction{
	{
		TransactionId:   "tx1",
		StoreId:         101,
		PaymentMethod:   1,
		VoucherId:       1001,
		UserId:          501,
		OriginalAmount:  200.0,
		DiscountApplied: 50.0,
		FinalAmount:     150.0,
		CreatedAt:       "2025-07-01 07:00:00",
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
		CreatedAt:       "2024-05-15 08:20:00",
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
		CreatedAt:       "2024-05-15 15:20:00",
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
		CreatedAt:       "2021-07-01 07:00:00",
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
		CreatedAt:       "2021-03-10 10:15:00",
	},
}
