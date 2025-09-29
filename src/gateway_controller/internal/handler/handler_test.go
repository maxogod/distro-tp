package handler_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/enum"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/gateway_controller/business"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/handler"
	"github.com/stretchr/testify/assert"
)

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
	result, err := th.HandleTask(enum.T1, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cleaned := result.([]raw.Transaction)
	if cleaned[0].VoucherId != 0 || cleaned[0].DiscountApplied != 0 || cleaned[0].PaymentMethod != 0 ||
		cleaned[0].OriginalAmount != 0 || cleaned[0].UserId != 0 || cleaned[0].StoreId != 0 {
		t.Errorf("fields not cleaned as expected for T1: VoucherId=%v, DiscountApplied=%v, PaymentMethod=%v, OriginalAmount=%v, UserId=%v, StoreId=%v", cleaned[0].VoucherId, cleaned[0].DiscountApplied, cleaned[0].PaymentMethod, cleaned[0].OriginalAmount, cleaned[0].UserId, cleaned[0].StoreId)
	}
	if cleaned[0].FinalAmount == 0 || cleaned[0].CreatedAt == "" {
		t.Errorf("required fields were cleaned for T1: FinalAmount=%v, CreatedAt=%v", cleaned[0].FinalAmount, cleaned[0].CreatedAt)
	}
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
	result, err := th.HandleTask(enum.T2, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cleaned := result.([]raw.TransactionItems)
	if cleaned[0].TransactionId != "" {
		t.Errorf("TransactionId should be cleaned for T2")
	}
	if cleaned[0].UnitPrice != 0 {
		t.Errorf("UnitPrice should be cleaned for T2")
	}
	// These should remain
	if cleaned[0].ItemId == 0 {
		t.Errorf("ItemId should not be cleaned for T2")
	}
	if cleaned[0].Quantity == 0 {
		t.Errorf("Quantity should not be cleaned for T2")
	}
	if cleaned[0].Subtotal == 0 {
		t.Errorf("Subtotal should not be cleaned for T2")
	}
	if cleaned[0].CreatedAt == "" {
		t.Errorf("CreatedAt should not be cleaned for T2")
	}
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
	result, err := th.HandleTask(enum.T3, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cleaned := result.([]raw.Transaction)
	assert.Equal(t, true, cleaned[0].VoucherId == 0, "VoucherId should be cleaned for T3")
	assert.Equal(t, true, cleaned[0].DiscountApplied == 0, "DiscountApplied should be cleaned for T3")
	assert.Equal(t, true, cleaned[0].PaymentMethod == 0, "PaymentMethod should be cleaned for T3")
	assert.Equal(t, true, cleaned[0].OriginalAmount == 0, "OriginalAmount should be cleaned for T3")
	assert.Equal(t, true, cleaned[0].UserId == 0, "UserId should be cleaned for T3")
	assert.Equal(t, true, cleaned[0].StoreId != 0, "StoreId should not be cleaned for T3")
	assert.Equal(t, true, cleaned[0].FinalAmount != 0, "FinalAmount should not be cleaned for T3")
	assert.Equal(t, true, cleaned[0].CreatedAt != "", "CreatedAt should not be cleaned for T3")
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
	result, err := th.HandleTask(enum.T4, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cleaned := result.([]raw.Transaction)
	assert.Equal(t, true, cleaned[0].VoucherId == 0, "VoucherId should be cleaned for T4")
	assert.Equal(t, true, cleaned[0].DiscountApplied == 0, "DiscountApplied should be cleaned for T4")
	assert.Equal(t, true, cleaned[0].PaymentMethod == 0, "PaymentMethod should be cleaned for T4")
	assert.Equal(t, true, cleaned[0].OriginalAmount == 0, "OriginalAmount should be cleaned for T4")
	assert.Equal(t, true, cleaned[0].StoreId != 0, "StoreId should not be cleaned for T4")
	assert.Equal(t, true, cleaned[0].UserId != 0, "UserId should not be cleaned for T4")
	assert.Equal(t, true, cleaned[0].FinalAmount != 0, "FinalAmount should not be cleaned for T4")
	assert.Equal(t, true, cleaned[0].CreatedAt != "", "CreatedAt should not be cleaned for T4")
}

func TestTaskHandler_UnknownTaskType(t *testing.T) {
	th := handler.NewTaskHandler(business.NewControllerService())
	_, err := th.HandleTask(enum.TaskType(99), []raw.Transaction{})
	if err == nil {
		t.Error("expected error for unknown task type, got nil")
	}
}
