package business

import (
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

type JoinerService struct{}

func NewFilterService() *JoinerService {
	return &JoinerService{}
}

// This is T2_1
func (js *JoinerService) JoinBestSellingProducts(bsp *reduced.BestSellingProducts) *joined.JoinBestSellingProducts {

	// ====================
	// ADD JOIN LOGIC HERE
	// ====================
	// add cache service as a atribuite to use here

	return &joined.JoinBestSellingProducts{}

}

// This is T2_2
func (js *JoinerService) JoinMostProfitsProducts(mpp *reduced.MostProfitsProducts) *joined.JoinMostProfitsProducts {

	// ====================
	// ADD JOIN LOGIC HERE
	// ====================
	// add cache service as a atribuite to use here

	return &joined.JoinMostProfitsProducts{}

}

// This is T3
func (js *JoinerService) JoinTPV(tpv *reduced.StoreTPV) *joined.JoinStoreTPV {

	// ====================
	// ADD JOIN LOGIC HERE
	// ====================
	// add cache service as a atribuite to use here

	return &joined.JoinStoreTPV{}

}

// This is T4
func (js *JoinerService) JoinMostPurchasesByUser(mpp *reduced.MostPurchasesUser) *joined.JoinMostPurchasesUser {

	// ====================
	// ADD JOIN LOGIC HERE
	// ====================
	// add cache service as a atribuite to use here

	return &joined.JoinMostPurchasesUser{}

}
