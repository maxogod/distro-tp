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

	// JoinTotalProfitBySubtotal is responsible for joining TotalProfitBySubtotal with MenuItem data
	// as part of T2_1 task.
	JoinTotalProfitBySubtotal(profit *reduced.TotalProfitBySubtotal, clientID string) (*reduced.TotalProfitBySubtotal, error)

	// JoinTotalSoldByQuantity is responsible for joining TotalSoldByQuantity with MenuItem data
	// as part of T2_2 task.
	JoinTotalSoldByQuantity(sales *reduced.TotalSoldByQuantity, clientID string) (*reduced.TotalSoldByQuantity, error)

	// JoinTotalPaymentValue is responsible for joining TotalPaymentValue with Store and User data
	// as part of T3 task.
	JoinTotalPaymentValue(tpv *reduced.TotalPaymentValue, clientID string) (*reduced.TotalPaymentValue, error)

	// JoinCountedUserTransactions is responsible for joining CountedUserTransactions with User data
	// as part of T4 task.
	JoinCountedUserTransactions(countedTransaction *reduced.CountedUserTransactions, clientID string) (*reduced.CountedUserTransactions, error)

	/* --- Resource release --- */

	// DeleteClientRefData deletes all reference data associated with the given clientID.
	DeleteClientRefData(clientID string) error

	// Close releases resources held by the service.
	Close() error
}
