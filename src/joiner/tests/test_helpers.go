package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maxogod/distro-tp/src/common/middleware"
	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/protocol"
	joiner "github.com/maxogod/distro-tp/src/joiner/business"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	RabbitURL = "amqp://guest:guest@localhost:5672/"
)

type TestCase struct {
	Queue         string
	DatasetType   models.RefDatasetType
	CsvPayloads   [][]byte
	ExpectedFiles []string
	TaskDone      models.TaskType
	SendDone      bool
}

func RunTest(t *testing.T, storeDir string, c TestCase) {
	t.Helper()

	j := StartJoiner(t, RabbitURL, storeDir, []string{c.Queue})
	defer func(j *joiner.Joiner) {
		err := j.Stop()
		assert.NoError(t, err)
	}(j)

	pub, err := middleware.NewQueueMiddleware(RabbitURL, c.Queue)
	assert.NoError(t, err)
	defer func() {
		_ = pub.Delete()
		_ = pub.Close()
	}()

	if c.DatasetType == models.Users {
		for _, csvPayload := range c.CsvPayloads {
			SendReferenceBatches(t, pub, [][]byte{csvPayload}, c.DatasetType)
		}
	} else {
		SendReferenceBatches(t, pub, c.CsvPayloads, c.DatasetType)
	}

	for i, expectedFile := range c.ExpectedFiles {
		if c.DatasetType == models.Users {
			AssertUsersAreTheExpected(t, expectedFile, [][]byte{c.CsvPayloads[i]}, c.DatasetType)
		} else if c.DatasetType == models.Stores {
			AssertStoresAreTheExpected(t, expectedFile, c.CsvPayloads, c.DatasetType)
		} else if c.DatasetType == models.MenuItems {
			AssertMenuItemsAreTheExpected(t, expectedFile, c.CsvPayloads, c.DatasetType)
		}
	}

	if c.SendDone {
		SendDoneMessage(t, pub, c.TaskDone)
	}
}

func SendReferenceBatches(t *testing.T, pub middleware.MessageMiddleware, csvPayloads [][]byte, datasetType models.RefDatasetType) {
	t.Helper()

	refBatch, err := getPayloadForDatasetType(t, datasetType, csvPayloads)
	assert.NoError(t, err)

	msgProto := &protocol.ReferenceQueueMessage{
		Payload: &protocol.ReferenceQueueMessage_ReferenceBatch{
			ReferenceBatch: refBatch,
		},
	}

	msgBytes, err := proto.Marshal(msgProto)
	assert.NoError(t, err)

	e := pub.Send(msgBytes)
	assert.Equal(t, 0, int(e))
}

func getPayloadForDatasetType(t *testing.T, datasetType models.RefDatasetType, csvPayloads [][]byte) (*protocol.ReferenceBatch, error) {
	var payload []byte
	var err error

	switch datasetType {
	case models.Users:
		payload, err = marshalPayloadForUsersDataset(t, csvPayloads)
	case models.Stores:
		payload, err = marshalPayloadForStoresDataset(t, csvPayloads)
	case models.MenuItems:
		payload, err = marshalPayloadForMenuItemsDataset(t, csvPayloads)
	default:
		return nil, fmt.Errorf("unknown dataset type: %d", datasetType)
	}

	return &protocol.ReferenceBatch{
		DatasetType: int32(datasetType),
		Payload:     payload,
	}, err
}

func marshalPayloadForUsersDataset(t *testing.T, csvPayloads [][]byte) ([]byte, error) {
	t.Helper()

	var users []*protocol.User
	for _, csvPayload := range csvPayloads {
		row := string(csvPayload)
		cols := strings.Split(strings.TrimSpace(row), ",")
		if len(cols) < 4 {
			t.Fatalf("invalid csv payload: %s", row)
		}

		userID, _ := strconv.Atoi(cols[0])
		birthdate := cols[2]
		registeredAt := cols[3]

		users = append(users, &protocol.User{
			UserId:       int32(userID),
			Gender:       cols[1],
			Birthdate:    birthdate,
			RegisteredAt: registeredAt,
		})
	}

	batch := &protocol.Users{Users: users}
	return proto.Marshal(batch)
}

func marshalPayloadForStoresDataset(t *testing.T, csvPayloads [][]byte) ([]byte, error) {
	t.Helper()

	var stores []*protocol.Store
	for _, csvPayload := range csvPayloads {
		fields := strings.Split(strings.TrimSpace(string(csvPayload)), ",")
		if len(fields) < 8 {
			return nil, fmt.Errorf("invalid store csv: %s", string(csvPayload))
		}

		storeID, _ := strconv.Atoi(fields[0])
		postal, _ := strconv.Atoi(fields[3])
		lat, _ := strconv.ParseFloat(fields[6], 64)
		lng, _ := strconv.ParseFloat(fields[7], 64)

		stores = append(stores, &protocol.Store{
			StoreID:    int32(storeID),
			StoreName:  fields[1],
			Street:     fields[2],
			PostalCode: int32(postal),
			City:       fields[4],
			State:      fields[5],
			Latitude:   lat,
			Longitude:  lng,
		})
	}

	batch := &protocol.Stores{Stores: stores}
	return proto.Marshal(batch)
}

func marshalPayloadForMenuItemsDataset(t *testing.T, csvPayloads [][]byte) ([]byte, error) {
	t.Helper()

	var items []*protocol.MenuItem
	for _, csvPayload := range csvPayloads {
		fields := strings.Split(strings.TrimSpace(string(csvPayload)), ",")
		if len(fields) < 7 {
			return nil, fmt.Errorf("invalid menu item csv: %s", string(csvPayload))
		}

		itemID, err := strconv.Atoi(fields[0])
		if err != nil {
			return nil, err
		}

		price, err := strconv.ParseFloat(fields[3], 64)
		if err != nil {
			return nil, err
		}

		isSeasonal := strings.ToLower(fields[4]) == "true"

		var availableFrom, availableTo *timestamppb.Timestamp
		if len(fields[5]) > 0 {
			timeAvailableFrom, timeErr := time.Parse("2006-01-02", fields[5])
			if timeErr != nil {
				return nil, fmt.Errorf("invalid available_from date: %s", fields[5])
			}
			availableFrom = timestamppb.New(timeAvailableFrom)
		}
		if len(fields[6]) > 0 {
			timeAvailableTo, timeErr := time.Parse("2006-01-02", fields[6])
			if timeErr != nil {
				return nil, fmt.Errorf("invalid available_to date: %s", fields[6])
			}
			availableTo = timestamppb.New(timeAvailableTo)
		}

		items = append(items, &protocol.MenuItem{
			ItemId:        int32(itemID),
			ItemName:      fields[1],
			Category:      fields[2],
			Price:         price,
			IsSeasonal:    isSeasonal,
			AvailableFrom: availableFrom,
			AvailableTo:   availableTo,
		})
	}

	batch := &protocol.MenuItems{Items: items}
	return proto.Marshal(batch)
}

func SendDoneMessage(t *testing.T, pub middleware.MessageMiddleware, datasetType models.TaskType) {
	doneMsg := &protocol.Done{
		TaskType: int32(datasetType),
	}

	msgProto := &protocol.ReferenceQueueMessage{
		Payload: &protocol.ReferenceQueueMessage_Done{
			Done: doneMsg,
		},
	}

	doneBytes, err := proto.Marshal(msgProto)
	assert.NoError(t, err)

	e := pub.Send(doneBytes)
	assert.Equal(t, 0, int(e))
}

func StartJoiner(t *testing.T, rabbitURL string, storeDir string, refQueueNames []string) *joiner.Joiner {
	t.Helper()

	joinerConfig := config.Config{
		GatewayAddress:              rabbitURL,
		StorePath:                   storeDir,
		StoreTPVQueue:               "store_tpv",
		TransactionCountedQueue:     "transaction_counted",
		TransactionSumQueue:         "transaction_sum",
		UserTransactionsQueue:       "user_transactions",
		JoinedTransactionsQueue:     "joined_transactions_queue",
		JoinedStoresTPVQueue:        "joined_stores_tpv_queue",
		JoinedUserTransactionsQueue: "joined_user_transactions_queue",
	}

	j := joiner.NewJoiner(&joinerConfig)

	for _, refQueueName := range refQueueNames {
		err := j.StartRefConsumer(refQueueName)
		assert.NoError(t, err)
	}

	return j
}

func AssertFileContainsPayloads[T any](
	t *testing.T,
	expectedFile string,
	csvPayloads [][]byte,
	datasetType models.RefDatasetType,
	unmarshalBatch func([]byte) ([]T, error),
	compare func(exp, got T, idx int, t *testing.T),
) {
	t.Helper()

	timeout := time.After(6 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	expectedRefBatch, err := getPayloadForDatasetType(t, datasetType, csvPayloads)
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

func PrepareDataBatch[T any](
	t *testing.T,
	taskType models.TaskType,
	items []*T,
	createContainer func([]*T) proto.Message,
) *protocol.DataBatch {
	t.Helper()

	container := createContainer(items)

	payload, err := proto.Marshal(container)
	assert.NoError(t, err)

	return &protocol.DataBatch{
		TaskType: int32(taskType),
		Payload:  payload,
	}
}

func PrepareStoreTPVBatch(t *testing.T, tpvs []*protocol.StoreTPV) *protocol.DataBatch {
	return PrepareDataBatch(t, 3, tpvs, func(items []*protocol.StoreTPV) proto.Message {
		return &protocol.StoresTPV{Items: items}
	})
}

func PrepareBestSellingBatch(t *testing.T, records []*protocol.BestSellingProducts, taskType models.TaskType) *protocol.DataBatch {
	return PrepareDataBatch(t, taskType, records, func(items []*protocol.BestSellingProducts) proto.Message {
		return &protocol.BestSellingProductsBatch{Items: items}
	})
}

func PrepareMostProfitsBatch(t *testing.T, records []*protocol.MostProfitsProducts, taskType models.TaskType) *protocol.DataBatch {
	return PrepareDataBatch(t, taskType, records, func(items []*protocol.MostProfitsProducts) proto.Message {
		return &protocol.MostProfitsProductsBatch{Items: items}
	})
}

func SendDataBatch(t *testing.T, inputQueue string, dataBatch *protocol.DataBatch) {
	t.Helper()

	pubProcessedData, err := middleware.NewQueueMiddleware(RabbitURL, inputQueue)
	assert.NoError(t, err)
	defer func() {
		_ = pubProcessedData.Close()
	}()

	dataMessage, err := proto.Marshal(dataBatch)
	assert.NoError(t, err)
	e := pubProcessedData.Send(dataMessage)
	assert.Equal(t, 0, int(e))
}

func GetOutputMessage(t *testing.T, outputQueue string) *protocol.DataBatch {
	t.Helper()

	consumer, err := middleware.NewQueueMiddleware(RabbitURL, outputQueue)
	assert.NoError(t, err)
	defer consumer.Close()

	var received *protocol.DataBatch
	done := make(chan struct{})

	consumer.StartConsuming(func(ch middleware.ConsumeChannel, d chan error) {
		for msg := range ch {
			var batch protocol.DataBatch
			unmErr := proto.Unmarshal(msg.Body, &batch)
			assert.NoError(t, unmErr)
			received = &batch
			err = msg.Ack(false)
			assert.NoError(t, err)
			d <- nil
			close(done)
			return
		}
	})

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("did not receive batch from %s", outputQueue)
	}

	return received
}

func AssertJoinedBatchIsTheExpected[T any, B proto.Message](
	t *testing.T,
	received *protocol.DataBatch,
	expected []*T,
	unmarshalBatch func([]byte) (B, []*T, error),
	compare func(exp, got *T, idx int, t *testing.T),
) {
	t.Helper()

	assert.NotNil(t, received, "received DataBatch should not be nil")

	// Deserialize into the right batch type
	_, items, err := unmarshalBatch(received.Payload)
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
	unmarshal := func(payload []byte) (*protocol.JoinStoreTPVBatch, []*protocol.JoinStoreTPV, error) {
		var batch protocol.JoinStoreTPVBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, nil, err
		}
		return &batch, batch.Items, nil
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
	unmarshal := func(payload []byte) (*protocol.JoinBestSellingProductsBatch, []*protocol.JoinBestSellingProducts, error) {
		var batch protocol.JoinBestSellingProductsBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, nil, err
		}
		return &batch, batch.Items, nil
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
	unmarshal := func(payload []byte) (*protocol.JoinMostProfitsProductsBatch, []*protocol.JoinMostProfitsProducts, error) {
		var batch protocol.JoinMostProfitsProductsBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			return nil, nil, err
		}
		return &batch, batch.Items, nil
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

	AssertFileContainsPayloads(t, expectedFile, csvPayloads, datasetType, unmarshal, compare)
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

	AssertFileContainsPayloads(t, expectedFile, csvPayloads, datasetType, unmarshal, compare)
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

	AssertFileContainsPayloads(t, expectedFile, csvPayloads, datasetType, unmarshal, compare)
}
