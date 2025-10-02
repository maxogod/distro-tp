package business

import (
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
	"github.com/maxogod/distro-tp/src/joiner/cache"
)

type JoinerService struct {
	refStore *cache.ReferenceDatasetStore
}

func NewFilterService(refStore *cache.ReferenceDatasetStore) *JoinerService {
	return &JoinerService{refStore: refStore}
}

// This is T2_1
func (js *JoinerService) JoinBestSellingProducts(batch *reduced.BestSellingProducts) (*joined.JoinBestSellingProducts, error) {

	menuItemsMap := js.refStore.Menu_items

	var joinedData *joined.JoinBestSellingProducts
	if item, ok := menuItemsMap[batch.ItemId]; ok {
		joinedData = &joined.JoinBestSellingProducts{
			YearMonthCreatedAt: batch.YearMonthCreatedAt,
			ItemName:           item.ItemName,
			SellingsQty:        batch.SellingsQty,
		}
	}

	return joinedData, nil
}

// This is T2_2
func (js *JoinerService) JoinMostProfitsProducts(batch *reduced.MostProfitsProducts) (*joined.JoinMostProfitsProducts, error) {
	menuItemsMap := js.refStore.Menu_items

	var joinedData *joined.JoinMostProfitsProducts
	if item, ok := menuItemsMap[batch.ItemId]; ok {
		joinedData = &joined.JoinMostProfitsProducts{
			YearMonthCreatedAt: batch.YearMonthCreatedAt,
			ItemName:           item.ItemName,
			ProfitSum:          batch.ProfitSum,
		}
	}

	return joinedData, nil
}

// This is T3
func (js *JoinerService) JoinTPV(batch *reduced.StoreTPV) (*joined.JoinStoreTPV, error) {
	storesMap := js.refStore.Stores

	var joinedData *joined.JoinStoreTPV
	if store, ok := storesMap[batch.StoreId]; ok {
		joinedData = &joined.JoinStoreTPV{
			YearHalfCreatedAt: batch.YearHalfCreatedAt,
			StoreName:         store.StoreName,
			Tpv:               batch.Tpv,
		}
	}
	return joinedData, nil
}

// This is T4
func (js *JoinerService) JoinMostPurchasesByUser(batch *reduced.MostPurchasesUser) (*joined.JoinMostPurchasesUser, error) {
	storesMap := js.refStore.Stores

	user := js.refStore.Users[batch.UserId]

	var joinedData *joined.JoinMostPurchasesUser
	store := storesMap[batch.StoreId]

	joinedData = &joined.JoinMostPurchasesUser{
		StoreName:     store.StoreName,
		UserBirthdate: user.Birthdate,
		PurchasesQty:  batch.PurchasesQty,
	}

	return joinedData, nil
}
