package business

import (
	"sort"

	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"google.golang.org/protobuf/proto"
)

var log = logger.GetLogger()

// Generic constraint that ensures
// the type implements our required methods
type TransactionCommon interface {
	GetCreatedAt() string
}

type AggregatorService struct{}

type MapJoinMostPurchasesUser map[string]*joined.JoinMostPurchasesUser
type MapTransactions map[string]*raw.Transaction
type MapJoinStoreTPV map[string]*joined.JoinStoreTPV
type MapJoinBestSelling map[string]*joined.JoinBestSellingProducts
type MapJoinMostProfits map[string]*joined.JoinMostProfitsProducts

func NewAggregatorService() *AggregatorService {
	return &AggregatorService{}
}

func AggregateDataTask1(refStore *cache.DataBatchStore) (MapTransactions, error) {
	finalAgg := make(MapTransactions)

	for _, currBatch := range refStore.GetBatches() {
		specificBatch := &raw.TransactionBatch{}
		if err := proto.Unmarshal(currBatch.Payload, specificBatch); err != nil {
			return nil, err
		}

		for _, transaction := range specificBatch.Transactions {
			finalAgg[transaction.TransactionId] = transaction
		}
	}
	log.Debugf("Final aggregation completed with %d items", len(finalAgg))

	return finalAgg, nil
}

func AggregateBestSellingData(refStore *cache.DataBatchStore) (MapJoinBestSelling, error) {
	finalAgg := make(MapJoinBestSelling)

	for _, currBatch := range refStore.GetBatches() {
		specificBatch := &joined.JoinBestSellingProductsBatch{}
		if err := proto.Unmarshal(currBatch.Payload, specificBatch); err != nil {
			return nil, err
		}

		for _, bestSelling := range specificBatch.Items {
			id := bestSelling.YearMonthCreatedAt + "|" + bestSelling.ItemName
			if existing, ok := finalAgg[id]; ok {
				existing.SellingsQty += bestSelling.SellingsQty
			} else {
				finalAgg[id] = bestSelling
			}

			finalAgg = top1BestSelling(finalAgg)
		}
	}
	log.Debugf("Final aggregation completed with %d items", len(finalAgg))

	return finalAgg, nil
}

func AggregateMostProfitsData(refStore *cache.DataBatchStore) (MapJoinMostProfits, error) {
	finalAgg := make(MapJoinMostProfits)

	for _, currBatch := range refStore.GetBatches() {
		specificBatch := &joined.JoinMostProfitsProductsBatch{}
		if err := proto.Unmarshal(currBatch.Payload, specificBatch); err != nil {
			return nil, err
		}

		for _, mostProfits := range specificBatch.Items {
			id := mostProfits.YearMonthCreatedAt + "|" + mostProfits.ItemName
			if existing, ok := finalAgg[id]; ok {
				existing.ProfitSum += mostProfits.ProfitSum
			} else {
				finalAgg[id] = mostProfits
			}

			finalAgg = top1MostProfits(finalAgg)
		}
	}
	log.Debugf("Final aggregation completed with %d items", len(finalAgg))

	return finalAgg, nil
}

func AggregateDataTask3(refStore *cache.DataBatchStore) (MapJoinStoreTPV, error) {
	finalAgg := make(MapJoinStoreTPV)

	for _, currBatch := range refStore.GetBatches() {
		specificBatch := &joined.JoinStoreTPVBatch{}
		if err := proto.Unmarshal(currBatch.Payload, specificBatch); err != nil {
			return nil, err
		}

		for _, storesTPV := range specificBatch.Items {
			id := storesTPV.YearHalfCreatedAt + "|" + storesTPV.StoreName
			if existing, ok := finalAgg[id]; ok {
				existing.Tpv += storesTPV.Tpv
			} else {
				finalAgg[id] = storesTPV
			}
		}
	}
	log.Debugf("Final aggregation completed with %d items", len(finalAgg))

	return finalAgg, nil
}

func AggregateDataTask4(refStore *cache.DataBatchStore) (MapJoinMostPurchasesUser, error) {
	finalAgg := make(MapJoinMostPurchasesUser)

	for _, currBatch := range refStore.GetBatches() {
		specificBatch := &joined.JoinMostPurchasesUserBatch{}
		if err := proto.Unmarshal(currBatch.Payload, specificBatch); err != nil {
			return nil, err
		}

		for _, purchaseUser := range specificBatch.Users {
			id := purchaseUser.StoreName + "|" + purchaseUser.UserBirthdate
			if existing, ok := finalAgg[id]; ok {
				existing.PurchasesQty += purchaseUser.PurchasesQty
			} else {
				finalAgg[id] = purchaseUser
			}

			finalAgg = top3ByStore(finalAgg)
		}
	}
	log.Debugf("Final aggregation completed with %d items", len(finalAgg))

	return finalAgg, nil
}

func topNByGroup[T proto.Message, M ~map[string]T](
	data M,
	groupKey func(T) string,
	topGroupKey func(T) string,
	sortCmp func(a, b T) bool,
	topN int,
) M {
	itemsByGroup := make(map[string][]T)
	for _, item := range data {
		key := groupKey(item)
		itemsByGroup[key] = append(itemsByGroup[key], item)
	}

	result := make(M)

	for _, items := range itemsByGroup {
		sort.Slice(items, func(i, j int) bool {
			return sortCmp(items[i], items[j])
		})

		limit := topN
		if len(items) < topN {
			limit = len(items)
		}

		for _, item := range items[:limit] {
			resultKey := topGroupKey(item)
			result[resultKey] = item
		}
	}

	return result
}

func top3ByStore(data MapJoinMostPurchasesUser) MapJoinMostPurchasesUser {
	return topNByGroup(data,
		func(user *joined.JoinMostPurchasesUser) string { return user.StoreName },
		func(user *joined.JoinMostPurchasesUser) string { return user.StoreName + "|" + user.UserBirthdate },
		func(userA, userB *joined.JoinMostPurchasesUser) bool {
			if userA.PurchasesQty == userB.PurchasesQty {
				return userA.UserBirthdate < userB.UserBirthdate
			}
			return userA.PurchasesQty > userB.PurchasesQty
		},
		3,
	)
}

func top1BestSelling(data MapJoinBestSelling) MapJoinBestSelling {
	return topNByGroup(data,
		func(u *joined.JoinBestSellingProducts) string { return u.YearMonthCreatedAt },
		func(u *joined.JoinBestSellingProducts) string { return u.YearMonthCreatedAt },
		func(a, b *joined.JoinBestSellingProducts) bool {
			if a.SellingsQty == b.SellingsQty {
				return a.ItemName < b.ItemName
			}
			return a.SellingsQty > b.SellingsQty
		},
		1,
	)
}

func top1MostProfits(data MapJoinMostProfits) MapJoinMostProfits {
	return topNByGroup(data,
		func(u *joined.JoinMostProfitsProducts) string { return u.YearMonthCreatedAt },
		func(u *joined.JoinMostProfitsProducts) string { return u.YearMonthCreatedAt },
		func(a, b *joined.JoinMostProfitsProducts) bool {
			if a.ProfitSum == b.ProfitSum {
				return a.ItemName < b.ItemName
			}
			return a.ProfitSum > b.ProfitSum
		},
		1,
	)
}
