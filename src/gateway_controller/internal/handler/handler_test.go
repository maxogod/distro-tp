package handler_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/gateway_controller/business"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/handler"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func marshalTransactionBatch(transactions []raw.Transaction) []byte {
	batch := &raw.TransactionBatch{Transactions: make([]*raw.Transaction, len(transactions))}
	for i := range transactions {
		batch.Transactions[i] = &transactions[i]
	}
	data, _ := proto.Marshal(batch)
	return data
}

func marshalTransactionItemsBatch(items []raw.TransactionItems) []byte {
	batch := &raw.TransactionItemsBatch{TransactionItems: make([]*raw.TransactionItems, len(items))}
	for i := range items {
		batch.TransactionItems[i] = &items[i]
	}
	data, _ := proto.Marshal(batch)
	return data
}

func TestTaskHandler_HandleTaskType1(t *testing.T) {
	th := handler.NewTaskHandler(business.NewControllerService())
	input := []raw.Transaction{
		{
			TransactionId:   "tx1",
			StoreId:         123,
			PaymentMethod:   2,
			VoucherId:       555,
			UserId:          42,
			OriginalAmount:  100.5,
			DiscountApplied: 10.0,
			FinalAmount:     90.5,
			CreatedAt:       "2025-09-28T10:00:00Z",
		},
	}
	payload := marshalTransactionBatch(input)
	err := th.HandleTask(enum.T1, payload)
	assert.NoError(t, err)
	// Unmarshal to check cleaned data
	var cleanedBatch raw.TransactionBatch
	_ = proto.Unmarshal(payload, &cleanedBatch)
	cleaned := cleanedBatch.Transactions[0]
	assert.Equal(t, int64(0), cleaned.VoucherId)
	assert.Equal(t, float64(0), cleaned.DiscountApplied)
	assert.Equal(t, int32(0), cleaned.PaymentMethod)
	assert.Equal(t, float64(0), cleaned.OriginalAmount)
	assert.Equal(t, int64(0), cleaned.UserId)
	assert.Equal(t, int64(0), cleaned.StoreId)
	assert.NotEqual(t, float64(0), cleaned.FinalAmount)
	assert.NotEmpty(t, cleaned.CreatedAt)
}

func TestTaskHandler_HandleTaskType2(t *testing.T) {
	th := handler.NewTaskHandler(business.NewControllerService())
	input := []raw.TransactionItems{
		{
			TransactionId: "tx2",
			ItemId:        1,
			Quantity:      5,
			UnitPrice:     20.0,
			Subtotal:      100.0,
			CreatedAt:     "2025-09-28T11:00:00Z",
		},
	}
	payload := marshalTransactionItemsBatch(input)
	err := th.HandleTask(enum.T2, payload)
	assert.NoError(t, err)
	var cleanedBatch raw.TransactionItemsBatch
	_ = proto.Unmarshal(payload, &cleanedBatch)
	cleaned := cleanedBatch.TransactionItems[0]
	assert.Empty(t, cleaned.TransactionId)
	assert.Equal(t, float64(0), cleaned.UnitPrice)
	assert.NotEqual(t, int64(0), cleaned.ItemId)
	assert.NotEqual(t, int32(0), cleaned.Quantity)
	assert.NotEqual(t, float64(0), cleaned.Subtotal)
	assert.NotEmpty(t, cleaned.CreatedAt)
}

func TestTaskHandler_HandleTaskType3(t *testing.T) {
	th := handler.NewTaskHandler(business.NewControllerService())
	input := []raw.Transaction{
		{
			TransactionId:   "tx3",
			StoreId:         321,
			PaymentMethod:   1,
			VoucherId:       111,
			UserId:          99,
			OriginalAmount:  200.0,
			DiscountApplied: 20.0,
			FinalAmount:     180.0,
			CreatedAt:       "2025-09-28T12:00:00Z",
		},
	}
	payload := marshalTransactionBatch(input)
	err := th.HandleTask(enum.T3, payload)
	assert.NoError(t, err)
	var cleanedBatch raw.TransactionBatch
	_ = proto.Unmarshal(payload, &cleanedBatch)
	cleaned := cleanedBatch.Transactions[0]
	assert.Equal(t, int64(0), cleaned.VoucherId)
	assert.Equal(t, float64(0), cleaned.DiscountApplied)
	assert.Equal(t, int32(0), cleaned.PaymentMethod)
	assert.Equal(t, float64(0), cleaned.OriginalAmount)
	assert.Equal(t, int64(0), cleaned.UserId)
	assert.NotEqual(t, int64(0), cleaned.StoreId)
	assert.NotEqual(t, float64(0), cleaned.FinalAmount)
	assert.NotEmpty(t, cleaned.CreatedAt)
}

func TestTaskHandler_HandleTaskType4(t *testing.T) {
	th := handler.NewTaskHandler(business.NewControllerService())
	input := []raw.Transaction{
		{
			TransactionId:   "tx4",
			StoreId:         222,
			PaymentMethod:   3,
			VoucherId:       333,
			UserId:          77,
			OriginalAmount:  300.0,
			DiscountApplied: 30.0,
			FinalAmount:     270.0,
			CreatedAt:       "2025-09-28T13:00:00Z",
		},
	}
	payload := marshalTransactionBatch(input)
	err := th.HandleTask(enum.T4, payload)
	assert.NoError(t, err)
	var cleanedBatch raw.TransactionBatch
	_ = proto.Unmarshal(payload, &cleanedBatch)
	cleaned := cleanedBatch.Transactions[0]
	assert.Equal(t, int64(0), cleaned.VoucherId)
	assert.Equal(t, float64(0), cleaned.DiscountApplied)
	assert.Equal(t, int32(0), cleaned.PaymentMethod)
	assert.Equal(t, float64(0), cleaned.OriginalAmount)
	assert.NotEqual(t, int64(0), cleaned.StoreId)
	assert.NotEqual(t, int64(0), cleaned.UserId)
	assert.NotEqual(t, float64(0), cleaned.FinalAmount)
	assert.NotEmpty(t, cleaned.CreatedAt)
}

func TestTaskHandler_UnknownTaskType(t *testing.T) {
	th := handler.NewTaskHandler(business.NewControllerService())
	payload := marshalTransactionBatch([]raw.Transaction{{TransactionId: "txX"}})
	err := th.HandleTask(enum.TaskType(99), payload)
	assert.Error(t, err, "expected error for unknown task type")
}
