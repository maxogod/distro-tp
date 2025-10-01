package service

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/maxogod/distro-tp/src/aggregator/cache"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"google.golang.org/protobuf/proto"
)

type MapJoinMostPurchasesUser map[string]*joined.JoinMostPurchasesUser
type MapTransactions map[string]*raw.Transaction
type MapJoinStoreTPV map[string]*joined.JoinStoreTPV
type MapJoinBestSelling map[string]*joined.JoinBestSellingProducts
type MapJoinMostProfits map[string]*joined.JoinMostProfitsProducts

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

	return finalAgg, nil
}

func (a *Aggregator) AggregateDataTask1() error {
	aggregatedData, err := aggregateTask(
		"task1",
		a.config.StorePath,
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

	if err != nil {
		return err
	}

	return a.SendAggregateDataTask1(aggregatedData)
}

func (a *Aggregator) AggregateDataTask2() error {
	err := a.aggregateBestSellingData()
	if err != nil {
		return err
	}
	return a.aggregateMostProfitsData()
}

func (a *Aggregator) aggregateBestSellingData() error {
	topBestSelling, err := aggregateTask(
		"task2_1",
		a.config.StorePath,
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

	if err != nil {
		return err
	}

	return a.SendAggregateDataBestSelling(topBestSelling)
}

func (a *Aggregator) aggregateMostProfitsData() error {
	topMostProfits, err := aggregateTask(
		"task2_2",
		a.config.StorePath,
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

	if err != nil {
		return err
	}

	return a.SendAggregateDataMostProfits(topMostProfits)
}

func (a *Aggregator) AggregateDataTask3() error {
	aggregatedData, err := aggregateTask(
		"task3",
		a.config.StorePath,
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

	if err != nil {
		return err
	}

	return a.SendAggregateDataTask3(aggregatedData)
}

func (a *Aggregator) AggregateDataTask4() error {
	topMostPurchases, err := aggregateTask(
		"task4",
		a.config.StorePath,
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

	if err != nil {
		return err
	}

	return a.SendAggregateDataTask4(topMostPurchases)
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
