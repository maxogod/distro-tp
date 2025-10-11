package business

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

type JoinerService interface {
	/* --- Store raw reference data --- */

	// StoreMenuItems stores menu items in the cache service.
	StoreMenuItems(clientID string, items []*raw.MenuItem) error

	// StoreStores stores stores info in the cache service.
	StoreStores(clientID string, items []*raw.Store) error

	// StoreUsers stores users in the cache service.
	StoreUsers(clientID string, items []*raw.User) error

	/* --- Get joined data --- */

	// GetJoinedTransactions retrieves joined transactions from the cache service.
	GetJoinedTransactions(transactions []*raw.Transaction, clientID string, amount int32) ([]*raw.Transaction, bool)

	// GetStoredTotalProfitBySubtotal retrieves stored total profit by subtotal from the cache service.
	GetStoredTotalProfitBySubtotal(profit *reduced.TotalProfitBySubtotal, clientID string, amount int32) ([]*reduced.TotalProfitBySubtotal, bool)

	// GetStoredTotalSoldByQuantity retrieves stored total sold by quantity from the cache service.
	GetStoredTotalSoldByQuantity(sales *reduced.TotalSoldByQuantity, clientID string, amount int32) ([]*reduced.TotalSoldByQuantity, bool)

	// GetStoredTotalPaymentValue retrieves stored total payment value from the cache service.
	GetStoredTotalPaymentValue(tpv *reduced.TotalPaymentValue, clientID string, amount int32) ([]*reduced.TotalPaymentValue, bool)

	// GetStoredCountedUserTransactions retrieves stored counted user transactions from the cache service.
	GetStoredCountedUserTransactions(countedTransaction *reduced.CountedUserTransactions, clientID string, amount int32) ([]*reduced.CountedUserTransactions, bool)

	/* --- Resource release --- */

	// DeleteClientRefData deletes all reference data associated with the given clientID.
	DeleteClientRefData(clientID string) error

	// Close releases resources held by the service.
	Close() error
}
