package business

import "github.com/maxogod/distro-tp/src/common/models/raw"

// FilterService defines the interface for filtering transaction data.
type FilterService interface {
	// FilterByYear filters a transaction or transaction_items batch based on the year.
	FilterByYear(*raw.TransactionBatch) error

	// FilterByYear filters a transaction or transaction_items batch based on the year.
	FilterItemsByYear(*raw.TransactionItemsBatch) error

	// FilterByTime filters a transaction_items batch based on the month.
	FilterByTime(*raw.TransactionBatch) error

	// FilterByFinalAmount filters a transaction batch based on the final amount.
	FilterByFinalAmount(*raw.TransactionBatch) error

	// FilterNullUserIDs filters a transaction batch to remove entries with null user IDs.
	FilterNullUserIDs(*raw.TransactionBatch) error
}
