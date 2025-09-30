package test_helpers

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/joined"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

type UnmarshalBatchFunc[T any] func(payload []byte) ([]*T, error)
type CompareFunc[T any] func(exp, got *T, idx int, t *testing.T)

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
					offset += 4 // tamaño del uint32 leído

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
	compare CompareFunc[T],
) {
	t.Helper()

	assert.NotNil(t, received, "received DataBatch should not be nil")

	items, err := unmarshalBatch(received.Payload)
	assert.NoError(t, err, "failed to unmarshal DataBatch.Payload")

	assert.Len(t, items, len(expected), "unexpected number of joined records")

	for i, exp := range expected {
		if i >= len(items) {
			t.Fatalf("expected at least %d items but got %d", len(expected), len(items))
		}
		got := items[i]
		compare(exp, got, i, t)
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

	compare := func(exp, got *joined.JoinMostPurchasesUser, idx int, t *testing.T) {
		assert.Equal(t, exp.StoreName, got.StoreName, "StoreName mismatch at index %d", idx)
		assert.Equal(t, exp.UserBirthdate, got.UserBirthdate, "UserBirthdate mismatch at index %d", idx)
		assert.Equal(t, exp.PurchasesQty, got.PurchasesQty, "PurchasesQty mismatch at index %d", idx)
	}

	assertJoinedBatchIsTheExpected(t, received, expected, unmarshal, compare)
}
