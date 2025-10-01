package test_helpers

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/models/controller_connection"
	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

type UnmarshalBatchFunc[T any] func(payload []byte) ([]*T, error)
type CompareFunc[T any] func(exp, got *T, idx int, t *testing.T)
type EqualFunc[T any] func(exp, got *T) bool

func assertBatchPersistence[T any](
	t *testing.T,
	expectedFile string,
	expectedItems []*T,
	unmarshalBatch UnmarshalBatchFunc[T],
	compare CompareFunc[T],
) {
	t.Helper()

	timeout := time.After(6 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for file %s", expectedFile)
		case <-tick.C:
			if _, fileErr := os.Stat(expectedFile); fileErr == nil {
				f, openErr := os.Open(expectedFile)
				assert.NoError(t, openErr)

				fileData, fileReadErr := io.ReadAll(f)
				assert.NoError(t, fileReadErr)
				closeErr := f.Close()
				assert.NoError(t, closeErr)

				var offset int
				for offset < len(fileData) {
					var length uint32
					readErr := binary.Read(bytes.NewReader(fileData[offset:]), binary.LittleEndian, &length)
					assert.NoError(t, readErr)
					offset += 4 // size of the read uint32

					if offset+int(length) > len(fileData) {
						t.Fatalf("invalid data length in file %s", expectedFile)
					}

					data := fileData[offset : offset+int(length)]
					offset += int(length)

					receivedItems, recvErr := unmarshalBatch(data)
					assert.NoError(t, recvErr)

					assert.Len(t, receivedItems, len(expectedItems), "number of items mismatch")
					for i := range expectedItems {
						compare(expectedItems[i], receivedItems[i], i, t)
					}
				}

				return
			}
		}
	}
}

func assertJoinedBatchIsTheExpected[T any](
	t *testing.T,
	received *data_batch.DataBatch,
	expected []*T,
	unmarshalBatch UnmarshalBatchFunc[T],
	equal EqualFunc[T],
) {
	t.Helper()

	assert.NotNil(t, received, "received DataBatch should not be nil")

	items, err := unmarshalBatch(received.Payload)
	assert.NoError(t, err, "failed to unmarshal DataBatch.Payload")

	assert.Len(t, items, len(expected), "unexpected number of joined records")

	for _, exp := range expected {
		found := false
		for _, got := range items {
			if equal(exp, got) {
				found = true
				break
			}
		}
		assert.True(t, found, "expected item %+v not found in received batch", exp)
	}
}

func AssertPersistentMostProfitsProducts(
	t *testing.T,
	expectedFile string,
	dataBatch *data_batch.DataBatch,
) {
	mostProfitsBatch := &joined.JoinMostProfitsProductsBatch{}
	err := proto.Unmarshal(dataBatch.Payload, mostProfitsBatch)
	assert.NoError(t, err)

	unmarshal := func(payload []byte) ([]*joined.JoinMostProfitsProducts, error) {
		batch := &data_batch.DataBatch{}
		if batchErr := proto.Unmarshal(payload, batch); batchErr != nil {
			return nil, batchErr
		}

		joinedBatch := &joined.JoinMostProfitsProductsBatch{}
		if batchErr := proto.Unmarshal(batch.Payload, joinedBatch); batchErr != nil {
			return nil, batchErr
		}

		return joinedBatch.Items, nil
	}

	compare := func(exp, got *joined.JoinMostProfitsProducts, idx int, t *testing.T) {
		t.Helper()

		assert.Equal(t, exp.YearMonthCreatedAt, got.YearMonthCreatedAt, "YearMonthCreatedAt mismatch at index %d", idx)
		assert.Equal(t, exp.ItemName, got.ItemName, "ItemName mismatch at index %d", idx)
		assert.Equal(t, exp.ProfitSum, got.ProfitSum, "ProfitSum mismatch at index %d", idx)
	}

	assertBatchPersistence(t, expectedFile, mostProfitsBatch.Items, unmarshal, compare)
}

func AssertPersistentBestSellingProducts(
	t *testing.T,
	expectedFile string,
	dataBatch *data_batch.DataBatch,
) {
	bestSellingBatch := &joined.JoinBestSellingProductsBatch{}
	err := proto.Unmarshal(dataBatch.Payload, bestSellingBatch)
	assert.NoError(t, err)

	unmarshal := func(payload []byte) ([]*joined.JoinBestSellingProducts, error) {
		batch := &data_batch.DataBatch{}
		if batchErr := proto.Unmarshal(payload, batch); batchErr != nil {
			return nil, batchErr
		}

		joinedBatch := &joined.JoinBestSellingProductsBatch{}
		if batchErr := proto.Unmarshal(batch.Payload, joinedBatch); batchErr != nil {
			return nil, batchErr
		}

		return joinedBatch.Items, nil
	}

	compare := func(exp, got *joined.JoinBestSellingProducts, idx int, t *testing.T) {
		t.Helper()

		assert.Equal(t, exp.YearMonthCreatedAt, got.YearMonthCreatedAt, "YearMonthCreatedAt mismatch at index %d", idx)
		assert.Equal(t, exp.ItemName, got.ItemName, "ItemName mismatch at index %d", idx)
		assert.Equal(t, exp.SellingsQty, got.SellingsQty, "SellingsQty mismatch at index %d", idx)
	}

	assertBatchPersistence(t, expectedFile, bestSellingBatch.Items, unmarshal, compare)
}

func AssertPersistentJoinedStoresTPV(
	t *testing.T,
	expectedFile string,
	dataBatch *data_batch.DataBatch,
) {
	storeTpvBatch := &joined.JoinStoreTPVBatch{}
	err := proto.Unmarshal(dataBatch.Payload, storeTpvBatch)
	assert.NoError(t, err)

	unmarshal := func(payload []byte) ([]*joined.JoinStoreTPV, error) {
		batch := &data_batch.DataBatch{}
		if batchErr := proto.Unmarshal(payload, batch); batchErr != nil {
			return nil, batchErr
		}

		joinedBatch := &joined.JoinStoreTPVBatch{}
		if batchErr := proto.Unmarshal(batch.Payload, joinedBatch); batchErr != nil {
			return nil, batchErr
		}

		return joinedBatch.Items, nil
	}

	compare := func(exp, got *joined.JoinStoreTPV, idx int, t *testing.T) {
		t.Helper()

		assert.Equal(t, exp.YearHalfCreatedAt, got.YearHalfCreatedAt, "YearHalfCreatedAt mismatch at index %d", idx)
		assert.Equal(t, exp.StoreName, got.StoreName, "StoreName mismatch at index %d", idx)
		assert.Equal(t, exp.Tpv, got.Tpv, "Tpv mismatch at index %d", idx)
	}

	assertBatchPersistence(t, expectedFile, storeTpvBatch.Items, unmarshal, compare)
}

func AssertPersistentJoinedUserTransactions(
	t *testing.T,
	expectedFile string,
	dataBatch *data_batch.DataBatch,
) {
	usersBatch := &joined.JoinMostPurchasesUserBatch{}
	err := proto.Unmarshal(dataBatch.Payload, usersBatch)
	assert.NoError(t, err)

	unmarshal := func(payload []byte) ([]*joined.JoinMostPurchasesUser, error) {
		batch := &data_batch.DataBatch{}
		if batchErr := proto.Unmarshal(payload, batch); batchErr != nil {
			return nil, batchErr
		}

		joinedBatch := &joined.JoinMostPurchasesUserBatch{}
		if batchErr := proto.Unmarshal(batch.Payload, joinedBatch); batchErr != nil {
			return nil, batchErr
		}

		return joinedBatch.Users, nil
	}

	compare := func(exp, got *joined.JoinMostPurchasesUser, idx int, t *testing.T) {
		t.Helper()

		assert.Equal(t, exp.StoreName, got.StoreName, "StoreName mismatch at index %d", idx)
		assert.Equal(t, exp.UserBirthdate, got.UserBirthdate, "UserBirthdate mismatch at index %d", idx)
		assert.Equal(t, exp.PurchasesQty, got.PurchasesQty, "PurchasesQty mismatch at index %d", idx)
	}

	assertBatchPersistence(t, expectedFile, usersBatch.Users, unmarshal, compare)
}

func AssertAggregatedMostPurchasesUsers(
	t *testing.T,
	received *data_batch.DataBatch,
	expected []*joined.JoinMostPurchasesUser,
) {
	t.Helper()

	unmarshal := func(payload []byte) ([]*joined.JoinMostPurchasesUser, error) {
		var batch joined.JoinMostPurchasesUserBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, err
		}
		return batch.Users, nil
	}

	equal := func(exp, got *joined.JoinMostPurchasesUser) bool {
		return exp.StoreName == got.StoreName &&
			exp.UserBirthdate == got.UserBirthdate &&
			exp.PurchasesQty == got.PurchasesQty
	}

	assertJoinedBatchIsTheExpected(t, received, expected, unmarshal, equal)
}

func AssertAggregatedStoresTPV(
	t *testing.T,
	received *data_batch.DataBatch,
	expected []*joined.JoinStoreTPV,
) {
	t.Helper()

	unmarshal := func(payload []byte) ([]*joined.JoinStoreTPV, error) {
		var batch joined.JoinStoreTPVBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, err
		}
		return batch.Items, nil
	}

	equal := func(exp, got *joined.JoinStoreTPV) bool {
		return exp.StoreName == got.StoreName &&
			exp.YearHalfCreatedAt == got.YearHalfCreatedAt &&
			exp.Tpv == got.Tpv
	}

	assertJoinedBatchIsTheExpected(t, received, expected, unmarshal, equal)
}

func AssertAggregatedBestSelling(
	t *testing.T,
	received *data_batch.DataBatch,
	expected []*joined.JoinBestSellingProducts,
) {
	t.Helper()

	unmarshal := func(payload []byte) ([]*joined.JoinBestSellingProducts, error) {
		var batch joined.JoinBestSellingProductsBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, err
		}
		return batch.Items, nil
	}

	equal := func(exp, got *joined.JoinBestSellingProducts) bool {
		return exp.ItemName == got.ItemName &&
			exp.YearMonthCreatedAt == got.YearMonthCreatedAt &&
			exp.SellingsQty == got.SellingsQty
	}

	assertJoinedBatchIsTheExpected(t, received, expected, unmarshal, equal)
}

func AssertAggregatedMostProfits(
	t *testing.T,
	received *data_batch.DataBatch,
	expected []*joined.JoinMostProfitsProducts,
) {
	t.Helper()

	unmarshal := func(payload []byte) ([]*joined.JoinMostProfitsProducts, error) {
		var batch joined.JoinMostProfitsProductsBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, err
		}
		return batch.Items, nil
	}

	equal := func(exp, got *joined.JoinMostProfitsProducts) bool {
		return exp.ItemName == got.ItemName &&
			exp.YearMonthCreatedAt == got.YearMonthCreatedAt &&
			exp.ProfitSum == got.ProfitSum
	}

	assertJoinedBatchIsTheExpected(t, received, expected, unmarshal, equal)
}

func AssertConnectionMsg(t *testing.T, gatewayControllerQueue string, finished bool) {
	initConnectionMsg := GetAllOutputMessages(t, gatewayControllerQueue, func(body []byte) (*controller_connection.ControllerConnection, error) {
		ctrl := &controller_connection.ControllerConnection{}
		if err := proto.Unmarshal(body, ctrl); err != nil {
			return nil, err
		}
		return ctrl, nil
	})[0]

	assert.Regexp(t, `^aggregator-.*`, initConnectionMsg.WorkerName)

	if finished {
		assert.True(t, initConnectionMsg.Finished)
	} else {
		assert.False(t, initConnectionMsg.Finished)
	}
}

func AssertAggregatedTransactions(
	t *testing.T,
	received *data_batch.DataBatch,
	expected []*raw.Transaction,
) {
	t.Helper()

	unmarshal := func(payload []byte) ([]*raw.Transaction, error) {
		var batch raw.TransactionBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, err
		}
		return batch.Transactions, nil
	}

	equal := func(exp, got *raw.Transaction) bool {
		return exp.TransactionId == got.TransactionId &&
			exp.StoreId == got.StoreId &&
			exp.PaymentMethod == got.PaymentMethod &&
			exp.VoucherId == got.VoucherId &&
			exp.UserId == got.UserId &&
			exp.OriginalAmount == got.OriginalAmount &&
			exp.DiscountApplied == got.DiscountApplied &&
			exp.FinalAmount == got.FinalAmount &&
			exp.CreatedAt == got.CreatedAt
	}

	assertJoinedBatchIsTheExpected(t, received, expected, unmarshal, equal)
}
