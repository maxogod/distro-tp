package business

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/models/transaction"
	"github.com/maxogod/distro-tp/src/common/models/transaction_items"
)

type GatewayControllerService struct {
	transactionCleaner map[string]func(*transaction.Transaction)
	itemCleaner        map[string]func(*transaction_items.TransactionItems)
}

func NewControllerService() *GatewayControllerService {

	// This map defines how to clean each field in the Transaction struct
	// in the respective clean methods, just pass the columns you want to "remove"
	transactionCleaner := map[string]func(*transaction.Transaction){
		"voucher_id":       func(t *transaction.Transaction) { t.VoucherId = 0 },
		"discount_applied": func(t *transaction.Transaction) { t.DiscountApplied = 0 },
		"user_id":          func(t *transaction.Transaction) { t.UserId = 0 },
		"store_id":         func(t *transaction.Transaction) { t.StoreId = 0 },
		"payment_method":   func(t *transaction.Transaction) { t.PaymentMethod = 0 },
		"original_amount":  func(t *transaction.Transaction) { t.OriginalAmount = 0 },
		"final_amount":     func(t *transaction.Transaction) { t.FinalAmount = 0 },
		"created_at":       func(t *transaction.Transaction) { t.CreatedAt = "" },
	}
	itemCleaner := map[string]func(*transaction_items.TransactionItems){
		"transaction_id": func(ti *transaction_items.TransactionItems) { ti.TransactionId = "" },
		"item_id":        func(ti *transaction_items.TransactionItems) { ti.ItemId = 0 },
		"quantity":       func(ti *transaction_items.TransactionItems) { ti.Quantity = 0 },
		"unit_price":     func(ti *transaction_items.TransactionItems) { ti.UnitPrice = 0 },
		"subtotal":       func(ti *transaction_items.TransactionItems) { ti.Subtotal = 0 },
		"created_at":     func(ti *transaction_items.TransactionItems) { ti.CreatedAt = "" },
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

func (s *GatewayControllerService) CleanTransactionData(transactionData []transaction.Transaction, removeColumns []string) ([]transaction.Transaction, error) {
	return cleanData(transactionData, s.transactionCleaner, removeColumns)
}

func (s *GatewayControllerService) CleanTransactionItemData(transactionItemData []transaction_items.TransactionItems, removeColumns []string) ([]transaction_items.TransactionItems, error) {
	return cleanData(transactionItemData, s.itemCleaner, removeColumns)
}
