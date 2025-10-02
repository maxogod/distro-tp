package business

import (
	"errors"
	"fmt"
	"io"
	"os"
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

func aggregateTask[T proto.Message, B proto.Message, M ~map[string]T](
	datasetName, storePath string,
	createSpecificBatch func() B,
	getItems func(B) []T,
	merge cache.MergeFunc[T],
	key cache.KeyFunc[T],
	combineTop func(M) M,
) (M, error) {
	filename := fmt.Sprintf("%s/%s.pb", storePath, datasetName)
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	finalAgg := make(M)

	for {
		currAgg, aggErr := cache.AggregateData(f, createSpecificBatch, getItems, merge, key)
		if aggErr != nil {
			if errors.Is(aggErr, io.EOF) {
				log.Debugf("Reached end of file")
				break
			}
			return nil, aggErr
		}

		for k, v := range currAgg {
			if existing, ok := finalAgg[k]; ok {
				finalAgg[k] = merge(existing, v)
			} else {
				finalAgg[k] = v
			}
		}

		finalAgg = combineTop(finalAgg)
	}
	log.Debugf("Final aggregation completed with %d items", len(finalAgg))

	return finalAgg, nil
}

func (a *AggregatorService) AggregateDataTask1(storePath string) (MapTransactions, error) {
	return aggregateTask(
		"task1",
		storePath,
		func() *raw.TransactionBatch { return &raw.TransactionBatch{} },
		func(batch *raw.TransactionBatch) []*raw.Transaction {
			return batch.Transactions
		},
		func(accumulated, incoming *raw.Transaction) *raw.Transaction {
			panic("This should not happen")
		},
		func(item *raw.Transaction) string {
			return item.TransactionId
		},
		func(m MapTransactions) MapTransactions { return m },
	)
}

func (a *AggregatorService) AggregateBestSellingData(storePath string) (MapJoinBestSelling, error) {
	return aggregateTask(
		"task2_1",
		storePath,
		func() *joined.JoinBestSellingProductsBatch {
			return &joined.JoinBestSellingProductsBatch{}
		},
		func(batch *joined.JoinBestSellingProductsBatch) []*joined.JoinBestSellingProducts {
			return batch.Items
		},
		func(accumulated, incoming *joined.JoinBestSellingProducts) *joined.JoinBestSellingProducts {
			accumulated.SellingsQty += incoming.SellingsQty
			return accumulated
		},
		func(item *joined.JoinBestSellingProducts) string {
			return item.YearMonthCreatedAt + "|" + item.ItemName
		},
		func(m MapJoinBestSelling) MapJoinBestSelling { return top1BestSelling(m) },
	)
}

func (a *AggregatorService) AggregateMostProfitsData(storePath string) (MapJoinMostProfits, error) {
	return aggregateTask(
		"task2_2",
		storePath,
		func() *joined.JoinMostProfitsProductsBatch {
			return &joined.JoinMostProfitsProductsBatch{}
		},
		func(batch *joined.JoinMostProfitsProductsBatch) []*joined.JoinMostProfitsProducts {
			return batch.Items
		},
		func(accumulated, incoming *joined.JoinMostProfitsProducts) *joined.JoinMostProfitsProducts {
			accumulated.ProfitSum += incoming.ProfitSum
			return accumulated
		},
		func(item *joined.JoinMostProfitsProducts) string {
			return item.YearMonthCreatedAt + "|" + item.ItemName
		},
		func(m MapJoinMostProfits) MapJoinMostProfits { return top1MostProfits(m) },
	)
}

func (a *AggregatorService) AggregateDataTask3(storePath string) (MapJoinStoreTPV, error) {
	return aggregateTask(
		"task3",
		storePath,
		func() *joined.JoinStoreTPVBatch {
			return &joined.JoinStoreTPVBatch{}
		},
		func(batch *joined.JoinStoreTPVBatch) []*joined.JoinStoreTPV {
			return batch.Items
		},
		func(accumulated, incoming *joined.JoinStoreTPV) *joined.JoinStoreTPV {
			accumulated.Tpv += incoming.Tpv
			return accumulated
		},
		func(item *joined.JoinStoreTPV) string {
			return item.YearHalfCreatedAt + "|" + item.StoreName
		},
		func(m MapJoinStoreTPV) MapJoinStoreTPV { return m },
	)
}

func (a *AggregatorService) AggregateDataTask4(storePath string) (MapJoinMostPurchasesUser, error) {
	return aggregateTask(
		"task4",
		storePath,
		func() *joined.JoinMostPurchasesUserBatch {
			return &joined.JoinMostPurchasesUserBatch{}
		},
		func(batch *joined.JoinMostPurchasesUserBatch) []*joined.JoinMostPurchasesUser {
			return batch.Users
		},
		func(accumulated, incoming *joined.JoinMostPurchasesUser) *joined.JoinMostPurchasesUser {
			accumulated.PurchasesQty += incoming.PurchasesQty
			return accumulated
		},
		func(item *joined.JoinMostPurchasesUser) string {
			return item.StoreName + "|" + item.UserBirthdate
		},
		func(m MapJoinMostPurchasesUser) MapJoinMostPurchasesUser { return top3ByStore(m) },
	)
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
