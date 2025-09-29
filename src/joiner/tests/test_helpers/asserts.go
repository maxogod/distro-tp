package test_helpers

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

type UnmarshalBatchFunc[T any] func(payload []byte) ([]*T, error)
type CompareFunc[T any] func(exp, got *T, idx int, t *testing.T)

func assertFileContainsPayloads[T any](
	t *testing.T,
	expectedFile string,
	csvPayloads [][]byte,
	datasetType models.RefDatasetType,
	unmarshalBatch UnmarshalBatchFunc[T],
	compare CompareFunc[T],
) {
	t.Helper()

	timeout := time.After(6 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	expectedRefBatch, err := GetPayloadForDatasetType(t, datasetType, csvPayloads)
	assert.NoError(t, err)
	expectedPayload, err := proto.Marshal(expectedRefBatch)
	assert.NoError(t, err)

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

					expectedItems, expErr := unmarshalBatch(expectedPayload)
					assert.NoError(t, expErr)

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

func AssertJoinerConsumed(t *testing.T, m middleware.MessageMiddleware, expected string) {
	t.Helper()

	time.Sleep(200 * time.Millisecond)

	found := false
	err := m.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
		for msg := range consumeChannel {
			if string(msg.Body) == expected {
				found = true
			}
			err := msg.Ack(false)
			assert.NoError(t, err)
			d <- nil
			break
		}
	})
	assert.Equal(t, 0, int(err))

	_ = m.StopConsuming()

	if found {
		t.Fatalf("message '%s' was not consumed by the joiner", expected)
	}
}

func AssertJoinedBatchIsTheExpected[T any](
	t *testing.T,
	received *protocol.DataBatch,
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

func AssertJoinedStoreTPVIsExpected(
	t *testing.T,
	received *protocol.DataBatch,
	expected []*protocol.JoinStoreTPV,
) {
	unmarshal := func(payload []byte) ([]*protocol.JoinStoreTPV, error) {
		var batch protocol.JoinStoreTPVBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, err
		}
		return batch.Items, nil
	}

	compare := func(exp, got *protocol.JoinStoreTPV, idx int, t *testing.T) {
		assert.Equal(t, exp.YearHalfCreatedAt, got.YearHalfCreatedAt, "YearHalfCreatedAt mismatch at index %d", idx)
		assert.Equal(t, exp.StoreName, got.StoreName, "StoreName mismatch at index %d", idx)
		assert.Equal(t, exp.Tpv, got.Tpv, "TPV mismatch at index %d", idx)
	}

	AssertJoinedBatchIsTheExpected(t, received, expected, unmarshal, compare)
}

func AssertJoinedBestSellingIsExpected(
	t *testing.T,
	received *protocol.DataBatch,
	expected []*protocol.JoinBestSellingProducts,
) {
	unmarshal := func(payload []byte) ([]*protocol.JoinBestSellingProducts, error) {
		var batch protocol.JoinBestSellingProductsBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, err
		}
		return batch.Items, nil
	}

	compare := func(exp, got *protocol.JoinBestSellingProducts, idx int, t *testing.T) {
		assert.Equal(t, exp.YearMonthCreatedAt, got.YearMonthCreatedAt, "YearMonthCreatedAt mismatch at index %d", idx)
		assert.Equal(t, exp.ItemName, got.ItemName, "ItemName mismatch at index %d", idx)
		assert.Equal(t, exp.SellingsQty, got.SellingsQty, "SellingsQty mismatch at index %d", idx)
	}

	AssertJoinedBatchIsTheExpected(t, received, expected, unmarshal, compare)
}

func AssertJoinedMostProfitsIsExpected(
	t *testing.T,
	received *protocol.DataBatch,
	expected []*protocol.JoinMostProfitsProducts,
) {
	unmarshal := func(payload []byte) ([]*protocol.JoinMostProfitsProducts, error) {
		var batch protocol.JoinMostProfitsProductsBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, err
		}
		return batch.Items, nil
	}

	compare := func(exp, got *protocol.JoinMostProfitsProducts, idx int, t *testing.T) {
		assert.Equal(t, exp.YearMonthCreatedAt, got.YearMonthCreatedAt, "YearMonthCreatedAt mismatch at index %d", idx)
		assert.Equal(t, exp.ItemName, got.ItemName, "ItemName mismatch at index %d", idx)
		assert.Equal(t, exp.ProfitSum, got.ProfitSum, "ProfitSum mismatch at index %d", idx)
	}

	AssertJoinedBatchIsTheExpected(t, received, expected, unmarshal, compare)
}

func AssertMenuItemsAreTheExpected(
	t *testing.T,
	expectedFile string,
	csvPayloads [][]byte,
	datasetType models.RefDatasetType,
) {
	unmarshal := func(payload []byte) ([]*protocol.MenuItem, error) {
		refBatch := &protocol.ReferenceBatch{}
		if err := proto.Unmarshal(payload, refBatch); err != nil {
			return nil, err
		}

		batch := &protocol.MenuItems{}
		if err := proto.Unmarshal(refBatch.Payload, batch); err != nil {
			return nil, err
		}

		return batch.Items, nil
	}

	compare := func(exp, got *protocol.MenuItem, idx int, t *testing.T) {
		t.Helper()

		assert.Equal(t, exp.ItemId, got.ItemId, "ItemId mismatch at index %d", idx)
		assert.Equal(t, exp.ItemName, got.ItemName, "ItemName mismatch at index %d", idx)
		assert.Equal(t, exp.Category, got.Category, "Category mismatch at index %d", idx)
		assert.Equal(t, exp.Price, got.Price, "Price mismatch at index %d", idx)
		assert.Equal(t, exp.IsSeasonal, got.IsSeasonal, "IsSeasonal mismatch at index %d", idx)

		if exp.AvailableFrom != nil && got.AvailableFrom != nil {
			assert.True(t, exp.AvailableFrom.AsTime().Equal(got.AvailableFrom.AsTime()),
				"AvailableFrom mismatch at index %d: expected %v, got %v", idx, exp.AvailableFrom.AsTime(), got.AvailableFrom.AsTime())
		} else {
			assert.Equal(t, exp.AvailableFrom, got.AvailableFrom, "AvailableFrom nil mismatch at index %d", idx)
		}

		if exp.AvailableTo != nil && got.AvailableTo != nil {
			assert.True(t, exp.AvailableTo.AsTime().Equal(got.AvailableTo.AsTime()),
				"AvailableTo mismatch at index %d: expected %v, got %v", idx, exp.AvailableTo.AsTime(), got.AvailableTo.AsTime())
		} else {
			assert.Equal(t, exp.AvailableTo, got.AvailableTo, "AvailableTo nil mismatch at index %d", idx)
		}
	}

	assertFileContainsPayloads(t, expectedFile, csvPayloads, datasetType, unmarshal, compare)
}

func AssertUsersAreTheExpected(
	t *testing.T,
	expectedFile string,
	csvPayloads [][]byte,
	datasetType models.RefDatasetType,
) {
	unmarshal := func(payload []byte) ([]*protocol.User, error) {
		refBatch := &protocol.ReferenceBatch{}
		if err := proto.Unmarshal(payload, refBatch); err != nil {
			return nil, err
		}

		batch := &protocol.Users{}
		if err := proto.Unmarshal(refBatch.Payload, batch); err != nil {
			return nil, err
		}

		return batch.Users, nil
	}

	compare := func(exp, got *protocol.User, idx int, t *testing.T) {
		t.Helper()

		assert.Equal(t, exp.UserId, got.UserId, "UserId mismatch at index %d", idx)
		assert.Equal(t, exp.Gender, got.Gender, "Gender mismatch at index %d", idx)
		assert.Equal(t, exp.Birthdate, got.Birthdate, "Birthdate mismatch at index %d", idx)
		assert.Equal(t, exp.RegisteredAt, got.RegisteredAt, "RegisteredAt mismatch at index %d", idx)
	}

	assertFileContainsPayloads(t, expectedFile, csvPayloads, datasetType, unmarshal, compare)
}

func AssertStoresAreTheExpected(
	t *testing.T,
	expectedFile string,
	csvPayloads [][]byte,
	datasetType models.RefDatasetType,
) {
	unmarshal := func(payload []byte) ([]*protocol.Store, error) {
		refBatch := &protocol.ReferenceBatch{}
		if err := proto.Unmarshal(payload, refBatch); err != nil {
			return nil, err
		}

		batch := &protocol.Stores{}
		if err := proto.Unmarshal(refBatch.Payload, batch); err != nil {
			return nil, err
		}

		return batch.Stores, nil
	}

	compare := func(exp, got *protocol.Store, idx int, t *testing.T) {
		t.Helper()

		assert.Equal(t, exp.StoreID, got.StoreID, "StoreID mismatch at index %d", idx)
		assert.Equal(t, exp.StoreName, got.StoreName, "StoreName mismatch at index %d", idx)
		assert.Equal(t, exp.Street, got.Street, "Street mismatch at index %d", idx)
		assert.Equal(t, exp.PostalCode, got.PostalCode, "PostalCode mismatch at index %d", idx)
		assert.Equal(t, exp.City, got.City, "City mismatch at index %d", idx)
		assert.Equal(t, exp.State, got.State, "State mismatch at index %d", idx)
		assert.Equal(t, exp.Latitude, got.Latitude, "Latitude mismatch at index %d", idx)
		assert.Equal(t, exp.Longitude, got.Longitude, "Longitude mismatch at index %d", idx)
	}

	assertFileContainsPayloads(t, expectedFile, csvPayloads, datasetType, unmarshal, compare)
}

func AssertJoinedMostPurchasesUsersIsExpected(
	t *testing.T,
	received *protocol.DataBatch,
	expected []*protocol.JoinMostPurchasesUser,
) {
	t.Helper()

	unmarshal := func(payload []byte) ([]*protocol.JoinMostPurchasesUser, error) {
		var batch protocol.JoinMostPurchasesUserBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, err
		}
		return batch.Users, nil
	}

	compare := func(exp, got *protocol.JoinMostPurchasesUser, idx int, t *testing.T) {
		expDate, err := time.Parse("2006-01-02", exp.UserBirthdate)
		assert.NoError(t, err, "Failed to parse expected UserBirthdate at index %d", idx)
		gotDate, err := time.Parse("2006-01-02", got.UserBirthdate)
		assert.NoError(t, err, "Failed to parse received UserBirthdate at index %d", idx)

		assert.Equal(t, exp.StoreName, got.StoreName, "StoreName mismatch at index %d", idx)
		assert.True(t, expDate.Equal(gotDate), "UserBirthdate mismatch at index %d", idx)
		assert.Equal(t, exp.PurchasesQty, got.PurchasesQty, "PurchasesQty mismatch at index %d", idx)
	}

	AssertJoinedBatchIsTheExpected(t, received, expected, unmarshal, compare)
}
