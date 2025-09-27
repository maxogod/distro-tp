package mock

import (
	"coffee-analisis/src/common/models"
	"time"
)

var MockTransactionsOutputT1 = map[string]models.Transaction{
	"tx1": {
		TransactionId:   "tx1",
		StoreId:         101,
		PaymentMethod:   1,
		VoucherId:       1001,
		UserId:          501,
		OriginalAmount:  200.0,
		DiscountApplied: 50.0,
		FinalAmount:     150.0,
		CreatedAt:       time.Date(2025, 11, 8, 10, 30, 0, 0, time.UTC),
	},
}

var MockTransactionsOutputT3 = map[string]models.Transaction{
	"tx1": {
		TransactionId:   "tx1",
		StoreId:         101,
		PaymentMethod:   1,
		VoucherId:       1001,
		UserId:          501,
		OriginalAmount:  200.0,
		DiscountApplied: 50.0,
		FinalAmount:     150.0,
		CreatedAt:       time.Date(2025, 11, 8, 10, 30, 0, 0, time.UTC),
	},
	"tx2": {
		TransactionId:   "tx2",
		StoreId:         102,
		PaymentMethod:   2,
		VoucherId:       1002,
		UserId:          502,
		OriginalAmount:  100.0,
		DiscountApplied: 25.0,
		FinalAmount:     50.0,
		CreatedAt:       time.Date(2024, 5, 15, 8, 20, 0, 0, time.UTC),
	},
}

var MockTransactionsOutputT4 = map[string]models.Transaction{
	"tx1": {
		TransactionId:   "tx1",
		StoreId:         101,
		PaymentMethod:   1,
		VoucherId:       1001,
		UserId:          501,
		OriginalAmount:  200.0,
		DiscountApplied: 50.0,
		FinalAmount:     150.0,
		CreatedAt:       time.Date(2025, 11, 8, 10, 30, 0, 0, time.UTC),
	},
	"tx2": {
		TransactionId:   "tx2",
		StoreId:         102,
		PaymentMethod:   2,
		VoucherId:       1002,
		UserId:          502,
		OriginalAmount:  100.0,
		DiscountApplied: 25.0,
		FinalAmount:     50.0,
		CreatedAt:       time.Date(2024, 5, 15, 8, 20, 0, 0, time.UTC),
	},
	"tx3": {
		TransactionId:   "tx3",
		StoreId:         102,
		PaymentMethod:   2,
		VoucherId:       1002,
		UserId:          502,
		OriginalAmount:  100.0,
		DiscountApplied: 25.0,
		FinalAmount:     50.0,
		CreatedAt:       time.Date(2024, 5, 15, 15, 20, 0, 0, time.UTC),
	},
}

var MockTransactionItemsOutputT2 = map[string]models.TransactionItem{
	"tx1": {
		TransactionId: "tx1",
		ItemId:        1,
		Quantity:      2,
		UnitPrice:     30.0,
		Subtotal:      60.0,
		CreatedAt:     time.Date(2025, 11, 8, 11, 15, 0, 0, time.UTC),
	},
	"tx2": {
		TransactionId: "tx2",
		ItemId:        2,
		Quantity:      1,
		UnitPrice:     25.0,
		Subtotal:      25.0,
		CreatedAt:     time.Date(2024, 6, 10, 16, 45, 0, 0, time.UTC),
	},
}
