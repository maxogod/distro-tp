package business

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"google.golang.org/protobuf/proto"
)

type AggregatorService interface {
	StoreTransactions(clientID string, transactions []*raw.Transaction) error
	StoreTotalProfitBySubtotal(clientID string, reducedData *reduced.TotalProfitBySubtotal) error
	StoreTotalSoldByQuantity(clientID string, reducedData *reduced.TotalSoldByQuantity) error
	StoreTotalPaymentValue(clientID string, reducedData *reduced.TotalPaymentValue) error
	StoreCountedUserTransactions(clientID string, reducedData *reduced.CountedUserTransactions) error
	GetStoredTransactions(clientID string, amount int32) ([]*raw.Transaction, bool)
	GetStoredTotalProfitBySubtotal(clientID string, amount int32) ([]*reduced.TotalProfitBySubtotal, bool)
	GetStoredTotalSoldByQuantity(clientID string, amount int32) ([]*reduced.TotalSoldByQuantity, bool)
	GetStoredTotalPaymentValue(clientID string, amount int32) ([]*reduced.TotalPaymentValue, bool)
	GetStoredCountedUserTransactions(clientID string, amount int32) ([]*reduced.CountedUserTransactions, bool)
	SortData(clientID string, sortFn func(a, b *proto.Message) bool) error
	Close() error
}
