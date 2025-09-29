package handler_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models"
	"github.com/maxogod/distro-tp/src/common/models/transaction"
	"github.com/maxogod/distro-tp/src/common/models/transaction_items"
	"github.com/maxogod/distro-tp/src/gateway_controller/business"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/handler"
)

func TestTaskHandler_HandleTaskType1(t *testing.T) {
	th := handler.NewTaskHandler(business.NewControllerService())
	input := []transaction.Transaction{
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
	result, err := th.HandleTask(models.T1, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cleaned := result.([]transaction.Transaction)
	if cleaned[0].VoucherId != 0 || cleaned[0].DiscountApplied != 0 || cleaned[0].PaymentMethod != 0 ||
		cleaned[0].OriginalAmount != 0 || cleaned[0].UserId != 0 || cleaned[0].StoreId != 0 {
		t.Errorf("fields not cleaned as expected for T1: %+v", cleaned[0])
	}
	if cleaned[0].FinalAmount == 0 || cleaned[0].CreatedAt == "" {
		t.Errorf("required fields were cleaned for T1: %+v", cleaned[0])
	}
}

func TestTaskHandler_HandleTaskType2(t *testing.T) {
	th := handler.NewTaskHandler(business.NewControllerService())
	input := []transaction_items.TransactionItems{
		{
			TransactionId: "tx2",
			ItemId:        1,
			Quantity:      5,
			UnitPrice:     20.0,
			Subtotal:      100.0,
			CreatedAt:     "2025-09-28T11:00:00Z",
		},
	}
	result, err := th.HandleTask(models.T2, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cleaned := result.([]transaction_items.TransactionItems)
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
	input := []transaction.Transaction{
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
	result, err := th.HandleTask(models.T3, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cleaned := result.([]transaction.Transaction)
	if cleaned[0].VoucherId != 0 || cleaned[0].DiscountApplied != 0 || cleaned[0].PaymentMethod != 0 ||
		cleaned[0].OriginalAmount != 0 || cleaned[0].UserId != 0 {
		t.Errorf("fields not cleaned as expected for T3: %+v", cleaned[0])
	}
	// These should remain
	if cleaned[0].StoreId == 0 {
		t.Errorf("StoreId should not be cleaned for T3")
	}
	if cleaned[0].FinalAmount == 0 {
		t.Errorf("FinalAmount should not be cleaned for T3")
	}
	if cleaned[0].CreatedAt == "" {
		t.Errorf("CreatedAt should not be cleaned for T3")
	}
}

func TestTaskHandler_HandleTaskType4(t *testing.T) {
	th := handler.NewTaskHandler(business.NewControllerService())
	input := []transaction.Transaction{
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
	result, err := th.HandleTask(models.T4, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cleaned := result.([]transaction.Transaction)
	if cleaned[0].VoucherId != 0 || cleaned[0].DiscountApplied != 0 || cleaned[0].PaymentMethod != 0 ||
		cleaned[0].OriginalAmount != 0 {
		t.Errorf("fields not cleaned as expected for T4: %+v", cleaned[0])
	}
	// These should remain
	if cleaned[0].StoreId == 0 {
		t.Errorf("StoreId should not be cleaned for T4")
	}
	if cleaned[0].UserId == 0 {
		t.Errorf("UserId should not be cleaned for T4")
	}
	if cleaned[0].FinalAmount == 0 {
		t.Errorf("FinalAmount should not be cleaned for T4")
	}
	if cleaned[0].CreatedAt == "" {
		t.Errorf("CreatedAt should not be cleaned for T4")
	}
}

func TestTaskHandler_UnknownTaskType(t *testing.T) {
	th := handler.NewTaskHandler(business.NewControllerService())
	_, err := th.HandleTask(models.TaskType(99), []transaction.Transaction{})
	if err == nil {
		t.Error("expected error for unknown task type, got nil")
	}
}
