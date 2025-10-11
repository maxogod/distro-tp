package business

import "github.com/maxogod/distro-tp/src/common/models/raw"

const (
	MinYear = 2024
	MaxYear = 2025

	MinFinalAmount = 75.0

	MinHour = 6
	MaxHour = 23
)

// FilterService defines the interface for filtering transaction data.
type FilterService interface {
	// FilterByYear filters a transaction or transaction_items batch based on the year.
	FilterByYear(*raw.TransactionBatch) error

	// FilterByTime filters a transaction_items batch based on the month.
	FilterByTime(*raw.TransactionBatch) error

	// FilterByFinalAmount filters a transaction batch based on the final amount.
	FilterByFinalAmount(*raw.TransactionBatch) error
}
