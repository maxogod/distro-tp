package business_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/transaction"
	"github.com/maxogod/distro-tp/src/common/models/transaction_items"
	"github.com/maxogod/distro-tp/src/gateway_controller/business"
	"github.com/stretchr/testify/assert"
)

func TestCleanTransactionData_RemovesFields(t *testing.T) {
	svc := business.NewControllerService()
	data := []transaction.Transaction{
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
	remove := []string{"store_id", "user_id", "created_at"}

	cleaned, err := svc.CleanTransactionData(data, remove)
	assert.True(t, err == nil, "unexpected error: %v", err)
	assert.True(t, cleaned[0].StoreId == 0, "expected StoreId to be 0, got %d", cleaned[0].StoreId)
	assert.True(t, cleaned[0].UserId == 0, "expected UserId to be 0, got %d", cleaned[0].UserId)
	assert.True(t, cleaned[0].CreatedAt == "", "expected CreatedAt to be empty, got %s", cleaned[0].CreatedAt)
}

func TestCleanTransactionItemData_RemovesFields(t *testing.T) {
	svc := business.NewControllerService()
	data := []transaction_items.TransactionItems{
		{
			TransactionId: "tx1",
			ItemId:        1,
			Quantity:      5,
			UnitPrice:     20.0,
			Subtotal:      100.0,
			CreatedAt:     "2025-09-28T10:00:00Z",
		},
	}
	remove := []string{"item_id", "quantity", "created_at"}

	cleaned, err := svc.CleanTransactionItemData(data, remove)
	assert.True(t, err == nil, "unexpected error: %v", err)
	assert.True(t, cleaned[0].ItemId == 0, "expected ItemId to be 0, got %d", cleaned[0].ItemId)
	assert.True(t, cleaned[0].Quantity == 0, "expected Quantity to be 0, got %d", cleaned[0].Quantity)
	assert.True(t, cleaned[0].CreatedAt == "", "expected CreatedAt to be empty, got %s", cleaned[0].CreatedAt)
}

func TestCleanTransactionData_UnknownColumnError(t *testing.T) {
	svc := business.NewControllerService()
	data := []transaction.Transaction{
		{TransactionId: "tx1"},
	}
	remove := []string{"not_a_field"}

	_, err := svc.CleanTransactionData(data, remove)
	assert.True(t, err != nil, "expected error for unknown column, got nil")
}
