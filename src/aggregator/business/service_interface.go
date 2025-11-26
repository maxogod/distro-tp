package business

import (
	"github.com/maxogod/distro-tp/src/common/models/protocol"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

// AggregatorService defines the interface for storing and retrieving raw and reduced-aggregated data.
type AggregatorService interface {

	// ------------ Store Functions -----------

	// StoreData stores raw data envelopes
	StoreData(clientID string, data []*protocol.DataEnvelope) error

	// ------------ Retreival Functions -----------

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
