package models

import "time"

type Transaction struct {
	TransactionId   string
	StoreId         int64
	PaymentMethod   int
	VoucherId       int64
	UserId          int64
	OriginalAmount  float64
	DiscountApplied float64
	FinalAmount     float64
	CreatedAt       time.Time
}
