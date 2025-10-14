package business

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

type JoinerService interface {
	/* --- Store raw reference data --- */

	// StoreMenuItems stores menu items in the cache service.
	StoreMenuItems(clientID string, items []*raw.MenuItem) error

	// StoreShops stores stores info in the cache service.
	StoreShops(clientID string, items []*raw.Store) error

	// StoreUsers stores users in the cache service.
	StoreUsers(clientID string, items []*raw.User) error

	// FinishStoringRefData marks the completion of reference data storage for a client.
	FinishStoringRefData(clientID string) error

	/* --- Join Data --- */

	// JoinTotalProfitBySubtotal retrieves stored total profit by subtotal from the cache service.
	JoinTotalProfitBySubtotal(profit *reduced.TotalProfitBySubtotal, clientID string) []*reduced.TotalProfitBySubtotal

	// JoinTotalSoldByQuantity retrieves stored total sold by quantity from the cache service.
	JoinTotalSoldByQuantity(sales *reduced.TotalSoldByQuantity, clientID string) []*reduced.TotalSoldByQuantity

	// JoinTotalPaymentValue retrieves stored total payment value from the cache service.
	JoinTotalPaymentValue(tpv *reduced.TotalPaymentValue, clientID string) []*reduced.TotalPaymentValue

	// JoinCountedUserTransactions retrieves stored counted user transactions from the cache service.
	JoinCountedUserTransactions(countedTransaction *reduced.CountedUserTransactions, clientID string) []*reduced.CountedUserTransactions

	/* --- Resource release --- */

	// DeleteClientRefData deletes all reference data associated with the given clientID.
	DeleteClientRefData(clientID string) error

	// Close releases resources held by the service.
	Close() error
}
