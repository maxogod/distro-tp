package business

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/joiner/cache"
)

var log = logger.GetLogger()

const SEPERATOR = "@"

type joinerService struct {
	cacheService cache.CacheService
}

func NewJoinerService(cacheService cache.CacheService) JoinerService {
	return &joinerService{
		cacheService: cacheService,
	}
}

/* --- Store raw reference data --- */

func (js *joinerService) StoreMenuItems(clientID string, items []*raw.MenuItem) error {
	return nil
}

func (js *joinerService) StoreStores(clientID string, items []*raw.Store) error {
	return nil
}

func (js *joinerService) StoreUsers(clientID string, items []*raw.User) error {
	return nil
}

/* --- Get joined data --- */

func (js *joinerService) GetJoinedTransactions(transactions []*raw.Transaction, clientID string, amount int32) ([]*raw.Transaction, bool) {
	return nil, false
}

func (js *joinerService) GetStoredTotalProfitBySubtotal(profit *reduced.TotalProfitBySubtotal, clientID string, amount int32) ([]*reduced.TotalProfitBySubtotal, bool) {
	return nil, false
}

func (js *joinerService) GetStoredTotalSoldByQuantity(sales *reduced.TotalSoldByQuantity, clientID string, amount int32) ([]*reduced.TotalSoldByQuantity, bool) {
	return nil, false
}

func (js *joinerService) GetStoredTotalPaymentValue(tpv *reduced.TotalPaymentValue, clientID string, amount int32) ([]*reduced.TotalPaymentValue, bool) {
	return nil, false
}

func (js *joinerService) GetStoredCountedUserTransactions(countedTransaction *reduced.CountedUserTransactions, clientID string, amount int32) ([]*reduced.CountedUserTransactions, bool) {
	return nil, false
}

/* --- Resource release --- */

func (js *joinerService) DeleteClientRefData(clientID string) error {
	return nil
}

func (js *joinerService) Close() error {
	return js.cacheService.Close()
}
