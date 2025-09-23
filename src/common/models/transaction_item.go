package models

import "time"

type TransactionItem struct {
	TransactionId string
	ItemId        int64
	Quantity      int
	UnitPrice     float64
	Subtotal      float64
	CreatedAt     time.Time
}
