package service

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"google.golang.org/protobuf/proto"
)

type MergeFunc[T proto.Message] func(accumulated, incoming T) T
type KeyFunc[T proto.Message] func(item T) string
type MapJoinMostPurchasesUser map[string]*joined.JoinMostPurchasesUser
type MapJoinStoreTPV map[string]*joined.JoinStoreTPV
type MapJoinBestSelling map[string]*joined.JoinBestSellingProducts
type MapJoinMostProfits map[string]*joined.JoinMostProfitsProducts

func aggregateData[T proto.Message, B proto.Message](
	f *os.File,
	createSpecificBatch func() B,
	getItems func(B) []T,
	merge MergeFunc[T],
	key KeyFunc[T],
) (map[string]T, error) {
	var length uint32
	if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	dataBatchBytes := make([]byte, length)
	if _, err := io.ReadFull(f, dataBatchBytes); err != nil {
		return nil, err
	}

	dataBatch := &data_batch.DataBatch{}
	err := proto.Unmarshal(dataBatchBytes, dataBatch)
	if err != nil {
		return nil, err
	}

	specificBatch := createSpecificBatch()
	if err = proto.Unmarshal(dataBatch.Payload, specificBatch); err != nil {
		return nil, err
	}

	aggregatedItems := make(map[string]T)
	for _, item := range getItems(specificBatch) {
		k := key(item)
		if existing, ok := aggregatedItems[k]; ok {
			aggregatedItems[k] = merge(existing, item)
		} else {
			aggregatedItems[k] = proto.Clone(item).(T)
		}
	}

	return aggregatedItems, nil
}

func (a *Aggregator) AggregateDataTask2() error {
	err := a.aggregateBestSellingData()
	if err != nil {
		return err
	}
	return a.aggregateMostProfitsData()
}

func aggregateTask[T proto.Message, B proto.Message, M ~map[string]T](
	datasetName, storePath string,
	createSpecificBatch func() B,
	getItems func(B) []T,
	merge MergeFunc[T],
	key KeyFunc[T],
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
		currAgg, err := aggregateData(f, createSpecificBatch, getItems, merge, key)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
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

func top3ByStore(data MapJoinMostPurchasesUser) MapJoinMostPurchasesUser {
	usersByStore := make(map[string][]*joined.JoinMostPurchasesUser)
	for _, item := range data {
		usersByStore[item.StoreName] = append(usersByStore[item.StoreName], item)
	}

	result := make(map[string]*joined.JoinMostPurchasesUser)

	for _, users := range usersByStore {
		sort.Slice(users, func(i, j int) bool {
			if users[i].PurchasesQty == users[j].PurchasesQty {
				return users[i].UserBirthdate < users[j].UserBirthdate
			}
			return users[i].PurchasesQty > users[j].PurchasesQty
		})

		limit := 3
		if len(users) < 3 {
			limit = len(users)
		}

		for _, user := range users[:limit] {
			k := user.StoreName + "|" + user.UserBirthdate
			result[k] = user
		}
	}

	return result
}

func top1BestSelling(data MapJoinBestSelling) MapJoinBestSelling {
	itemsByYearMonth := make(map[string][]*joined.JoinBestSellingProducts)
	for _, item := range data {
		itemsByYearMonth[item.YearMonthCreatedAt] = append(itemsByYearMonth[item.YearMonthCreatedAt], item)
	}

	result := make(map[string]*joined.JoinBestSellingProducts)

	for _, items := range itemsByYearMonth {
		sort.Slice(items, func(i, j int) bool {
			if items[i].SellingsQty == items[j].SellingsQty {
				return items[i].ItemName < items[j].ItemName
			}
			return items[i].SellingsQty > items[j].SellingsQty
		})

		if len(items) > 0 {
			item := items[0]
			k := item.YearMonthCreatedAt + "|" + item.ItemName
			result[k] = item
		}
	}

	return result
}

func top1MostProfits(data MapJoinMostProfits) MapJoinMostProfits {
	itemsByYearMonth := make(map[string][]*joined.JoinMostProfitsProducts)
	for _, item := range data {
		itemsByYearMonth[item.YearMonthCreatedAt] = append(itemsByYearMonth[item.YearMonthCreatedAt], item)
	}

	result := make(map[string]*joined.JoinMostProfitsProducts)

	for _, items := range itemsByYearMonth {
		sort.Slice(items, func(i, j int) bool {
			if items[i].ProfitSum == items[j].ProfitSum {
				return items[i].ItemName < items[j].ItemName
			}
			return items[i].ProfitSum > items[j].ProfitSum
		})

		if len(items) > 0 {
			item := items[0]
			k := item.YearMonthCreatedAt + "|" + item.ItemName
			result[k] = item
		}
	}

	return result
}
