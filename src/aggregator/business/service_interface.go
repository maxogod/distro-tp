package business

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

// AggregatorService defines the interface for storing and retrieving raw and reduced-aggregated data.
type AggregatorService interface {

	// StoreTransactions stores raw transactions for a client as part of T1 task.
	StoreTransactions(clientID string, transactions []*raw.Transaction) error

	// StoreTotalProfitBySubtotal stores reduced TotalProfitBySubtotal data for a client as part of T2_1 task.
	StoreTotalProfitBySubtotal(clientID string, reducedData *reduced.TotalProfitBySubtotal) error

	// StoreTotalSoldByQuantity stores reduced TotalSoldByQuantity data for a client as part of T2_2 task.
	StoreTotalSoldByQuantity(clientID string, reducedData *reduced.TotalSoldByQuantity) error

	// StoreTotalPaymentValue stores reduced TotalPaymentValue data for a client as part of T3 task.
	StoreTotalPaymentValue(clientID string, reducedData *reduced.TotalPaymentValue) error

	// StoreCountedUserTransactions stores reduced CountedUserTransactions data for a client as part of T4 task.
	StoreCountedUserTransactions(clientID string, reducedData *reduced.CountedUserTransactions) error

	// GetStoredTransactions retrieves stored raw transactions for a client as part of T1 task.
	GetStoredTransactions(clientID string) ([]*raw.Transaction, error)

	// GetStoredTotalProfitBySubtotal retrieves stored reduced TotalProfitBySubtotal data for a client as part of T2_1 task.
	GetStoredTotalProfitBySubtotal(clientID string) ([]*reduced.TotalProfitBySubtotal, error)

	// GetStoredTotalSoldByQuantity retrieves stored reduced TotalSoldByQuantity data for a client as part of T2_2 task.
	GetStoredTotalSoldByQuantity(clientID string) ([]*reduced.TotalSoldByQuantity, error)

	// GetStoredTotalPaymentValue retrieves stored reduced TotalPaymentValue data for a client as part of T3 task.
	GetStoredTotalPaymentValue(clientID string) ([]*reduced.TotalPaymentValue, error)

	// GetStoredCountedUserTransactions retrieves stored reduced CountedUserTransactions data for a client as part of T4 task.
	GetStoredCountedUserTransactions(clientID string) (map[string][]*reduced.CountedUserTransactions, error)

	// FinishData finalizes and cleans up resources for a client's data.
	FinishData(clientID string) error

	// Close releases resources held by the service.
	Close() error
}
