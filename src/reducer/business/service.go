package business

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

type ReducerService struct{}

func NewReduceCountService() *ReducerService {
	return &ReducerService{}
}

// This represents T2_1
func (r *ReducerService) SumMostProfitsProducts(items []*raw.TransactionItems) *reduced.BestSellingProducts {

	// Since i get this data from the group_by worker
	// i can assume that the data in this batch is already grouped by userId and storeId
	itemID := items[0].GetItemId()
	yearMonthDate := items[0].GetCreatedAt()

	sellingQuantity := int64(0)
	for _, item := range items {
		sellingQuantity += int64(item.GetQuantity())
	}

	mostProfitsProducts := &reduced.BestSellingProducts{
		ItemId:             int32(itemID),
		YearMonthCreatedAt: yearMonthDate,
		SellingsQty:        sellingQuantity,
	}

	return mostProfitsProducts
}

// This represents T2_2
func (r *ReducerService) SumBestSellingProducts(items []*raw.TransactionItems) *reduced.MostProfitsProducts {

	// Since i get this data from the group_by worker
	// i can assume that the data in this batch is already grouped by userId and storeId
	itemID := items[0].GetItemId()
	yearMonthDate := items[0].GetCreatedAt()

	profitSum := float64(0)
	for _, item := range items {
		profitSum += float64(item.GetQuantity())
	}

	mostProfitsProducts := &reduced.MostProfitsProducts{
		ItemId:             int32(itemID),
		YearMonthCreatedAt: yearMonthDate,
		ProfitSum:          profitSum,
	}

	return mostProfitsProducts
}

// This represents T3
func (r *ReducerService) SumTPV(transactions []*raw.Transaction) *reduced.StoreTPV {

	// Since i get this data from the group_by worker
	// i can assume that the data in this batch is already grouped by userId and storeId
	storeId := transactions[0].GetStoreId()
	yearSemester := transactions[0].GetCreatedAt()

	finalAmount := int64(0)
	for _, tx := range transactions {
		finalAmount += int64(tx.GetFinalAmount())
	}

	result := &reduced.StoreTPV{
		StoreId:           int32(storeId),
		YearHalfCreatedAt: yearSemester,
		Tpv:               finalAmount,
	}

	return result
}

// This represents T4
func (r *ReducerService) CountMostPurchasesByPerson(transactions []*raw.Transaction) *reduced.MostPurchasesUser {

	// Since i get this data from the group_by worker
	// i can assume that the data in this batch is already grouped by userId and storeId
	userId := transactions[0].GetUserId()
	storeId := transactions[0].GetStoreId()

	result := &reduced.MostPurchasesUser{
		UserId:       int32(userId),
		StoreId:      int32(storeId),
		PurchasesQty: int32(len(transactions)),
	}

	return result
}
