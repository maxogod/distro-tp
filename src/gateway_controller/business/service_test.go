package business_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/gateway_controller/business"
	"github.com/stretchr/testify/assert"
)

func TestCleanTransactionData_RemovesFields(t *testing.T) {
	svc := business.NewControllerService()
	data := []*raw.Transaction{
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
	assert.NoError(t, err, "unexpected error: %v", err)
	assert.Equal(t, int64(0), cleaned[0].StoreId, "expected StoreId to be 0, got %d", cleaned[0].StoreId)
	assert.Equal(t, int64(0), cleaned[0].UserId, "expected UserId to be 0, got %d", cleaned[0].UserId)
	assert.Equal(t, "", cleaned[0].CreatedAt, "expected CreatedAt to be empty, got %s", cleaned[0].CreatedAt)
}

func TestCleanTransactionItemData_RemovesFields(t *testing.T) {
	svc := business.NewControllerService()
	data := []*raw.TransactionItems{
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
	assert.NoError(t, err, "unexpected error: %v", err)
	assert.Equal(t, int64(0), cleaned[0].ItemId, "expected ItemId to be 0, got %d", cleaned[0].ItemId)
	assert.Equal(t, int32(0), cleaned[0].Quantity, "expected Quantity to be 0, got %d", cleaned[0].Quantity)
	assert.Equal(t, "", cleaned[0].CreatedAt, "expected CreatedAt to be empty, got %s", cleaned[0].CreatedAt)
}

func TestCleanTransactionData_UnknownColumnError(t *testing.T) {
	svc := business.NewControllerService()
	data := []*raw.Transaction{
		{TransactionId: "tx1"},
	}
	remove := []string{"not_a_field"}

	_, err := svc.CleanTransactionData(data, remove)
	assert.Error(t, err, "expected error for unknown column, got nil")
}
