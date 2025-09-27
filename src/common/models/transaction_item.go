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

func (ti TransactionItem) GetCreatedAt() time.Time {
	return ti.CreatedAt
}

func (ti TransactionItem) IsEqual(other TransactionItem) bool {
	return ti.TransactionId == other.TransactionId &&
		ti.ItemId == other.ItemId &&
		ti.Quantity == other.Quantity &&
		ti.UnitPrice == other.UnitPrice &&
		ti.Subtotal == other.Subtotal &&
		ti.CreatedAt.Equal(other.CreatedAt)
}
