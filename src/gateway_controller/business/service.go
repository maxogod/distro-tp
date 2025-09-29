package business

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/raw"
)

type GatewayControllerService struct {
	transactionCleaner map[string]func(*raw.Transaction)
	itemCleaner        map[string]func(*raw.TransactionItems)
}

func NewControllerService() *GatewayControllerService {

	// This map defines how to clean each field in the Transaction struct
	// in the respective clean methods, just pass the columns you want to "remove"
	transactionCleaner := map[string]func(*raw.Transaction){
		"voucher_id":       func(t *raw.Transaction) { t.VoucherId = 0 },
		"discount_applied": func(t *raw.Transaction) { t.DiscountApplied = 0 },
		"user_id":          func(t *raw.Transaction) { t.UserId = 0 },
		"store_id":         func(t *raw.Transaction) { t.StoreId = 0 },
		"payment_method":   func(t *raw.Transaction) { t.PaymentMethod = 0 },
		"original_amount":  func(t *raw.Transaction) { t.OriginalAmount = 0 },
		"final_amount":     func(t *raw.Transaction) { t.FinalAmount = 0 },
		"created_at":       func(t *raw.Transaction) { t.CreatedAt = "" },
	}
	itemCleaner := map[string]func(*raw.TransactionItems){
		"transaction_id": func(ti *raw.TransactionItems) { ti.TransactionId = "" },
		"item_id":        func(ti *raw.TransactionItems) { ti.ItemId = 0 },
		"quantity":       func(ti *raw.TransactionItems) { ti.Quantity = 0 },
		"unit_price":     func(ti *raw.TransactionItems) { ti.UnitPrice = 0 },
		"subtotal":       func(ti *raw.TransactionItems) { ti.Subtotal = 0 },
		"created_at":     func(ti *raw.TransactionItems) { ti.CreatedAt = "" },
	}

	return &GatewayControllerService{
		transactionCleaner: transactionCleaner,
		itemCleaner:        itemCleaner,
	}
}

func cleanData[T any](data []T, cleanerMap map[string]func(*T), columns []string) ([]T, error) {
	for i := range data {
		for _, col := range columns {
			cleaner, ok := cleanerMap[col]
			if !ok {
				return nil, fmt.Errorf("unknown column for cleaning: %s", col)
			}
			cleaner(&data[i])
		}
	}
	return data, nil
}

func (s *GatewayControllerService) CleanTransactionData(transactionData []raw.Transaction, removeColumns []string) ([]raw.Transaction, error) {
	return cleanData(transactionData, s.transactionCleaner, removeColumns)
}

func (s *GatewayControllerService) CleanTransactionItemData(transactionItemData []raw.TransactionItems, removeColumns []string) ([]raw.TransactionItems, error) {
	return cleanData(transactionItemData, s.itemCleaner, removeColumns)
}
