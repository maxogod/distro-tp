package business

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

// AggregatorService defines the interface for storing and retrieving raw and reduced-aggregated data.
type AggregatorService interface {

	// ------------ Store Functions -----------

	// StoreTransactions stores raw transactions for a client as part of T1 task.
	StoreTransactions(clientID string, transactions []*raw.Transaction) error

	// StoreTotalProfitBySubtotal stores reduced TotalSumItem data for a client as part of T2_1 task.
	StoreTotalItems(clientID string, reducedData *reduced.TotalSumItem) error

	// StoreTotalPaymentValue stores reduced TotalPaymentValue data for a client as part of T3 task.
	StoreTotalPaymentValue(clientID string, reducedData *reduced.TotalPaymentValue) error

	// StoreCountedUserTransactions stores reduced CountedUserTransactions data for a client as part of T4 task.
	StoreCountedUserTransactions(clientID string, reducedData *reduced.CountedUserTransactions) error

	// ------------ Retreival Functions -----------

	// GetStoredTransactions retrieves stored raw transactions for a client as part of T1 task.
	GetStoredTransactions(clientID string) ([]*raw.Transaction, error)

	// GetStoredTotalItems retrieves stored reduced TotalSumItem data for a client as part of T2 task.
	GetStoredTotalItems(clientID string) ([]*reduced.TotalSumItem, []*reduced.TotalSumItem, error)

	// GetStoredTotalPaymentValue retrieves stored reduced TotalPaymentValue data for a client as part of T3 task.
	GetStoredTotalPaymentValue(clientID string) ([]*reduced.TotalPaymentValue, error)

	// GetStoredCountedUserTransactions retrieves stored reduced CountedUserTransactions data for a client as part of T4 task.
	GetStoredCountedUserTransactions(clientID string) (map[string][]*reduced.CountedUserTransactions, error)

	// RemoveData finalizes and cleans up resources for a client's data.
	RemoveData(clientID string) error

	// Close releases resources held by the service.
	Close() error
}
