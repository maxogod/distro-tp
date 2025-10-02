package handler_test

// import (
// 	"testing"

// 	"github.com/maxogod/distro-tp/src/common/models"
// 	"github.com/maxogod/distro-tp/src/common/models/transaction"
// 	"github.com/maxogod/distro-tp/src/common/models/transaction_items"
// 	"github.com/maxogod/distro-tp/src/filter/business"
// 	"github.com/maxogod/distro-tp/src/filter/internal/handler"
// 	"github.com/maxogod/distro-tp/src/filter/test/mock"
// 	"github.com/stretchr/testify/assert"
// )

// var taskConfig = &handler.TaskConfig{
// 	FilterYearFrom:       2024,
// 	FilterYearTo:         2025,
// 	BusinessHourFrom:     6,
// 	BusinessHourTo:       11,
// 	TotalAmountThreshold: 75.0,
// }

// func TestNewTaskHandler(t *testing.T) {
// 	filterService := business.NewFilterService()
// 	th := handler.NewTaskHandler(filterService, taskConfig)

// 	if th == nil {
// 		t.Fatal("NewTaskHandler() returned nil")
// 	}
// }

// func TestTaskHandler_HandleTransactionTasksCorrectly(t *testing.T) {
// 	filterService := business.NewFilterService()
// 	th := handler.NewTaskHandler(filterService, taskConfig)

// 	tests := []struct {
// 		name        string
// 		taskType    models.TaskType
// 		payload     []*transaction.Transaction
// 		expected    map[string]*transaction.Transaction
// 		errorString string
// 	}{
// 		{
// 			name:     "T1 - Filter transactions by year between 2024-2025, business hours between 6-11 hs, and total amount > 75.0",
// 			taskType: models.T1,
// 			payload:  mock.MockTransactions,
// 			expected: mock.MockTransactionsOutputT1,
// 		},
// 		{
// 			name:     "T3 - Filter transactions by year between 2024-2025 and by business hours between 6-11 hs",
// 			taskType: models.T3,
// 			payload:  mock.MockTransactions,
// 			expected: mock.MockTransactionsOutputT3,
// 		},
// 		{
// 			name:     "T4 - Filter transactions by year between 2024-2025",
// 			taskType: models.T4,
// 			payload:  mock.MockTransactions,
// 			expected: mock.MockTransactionsOutputT4,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			result, err := th.HandleTask(tt.taskType, tt.payload)

// 			if err != nil {
// 				t.Errorf("Unexpected error: %v", err)
// 				return
// 			}

// 			resultSlice := result.([]*transaction.Transaction)
// 			for _, actual := range resultSlice {
// 				exp, exists := tt.expected[actual.TransactionId]
// 				assert.Equal(t, true, exists, "TransactionId %s not found in expected results", actual.TransactionId)
// 				if exists {
// 					assert.Equal(t, exp, actual, "Transaction data does not match for TransactionId %s", actual.TransactionId)
// 				}
// 			}
// 		})
// 	}
// }

// func TestTaskHandler_HandleTransactionItemsTasksCorrectly(t *testing.T) {
// 	filterService := business.NewFilterService()
// 	th := handler.NewTaskHandler(filterService, taskConfig)

// 	tests := []struct {
// 		name        string
// 		taskType    models.TaskType
// 		payload     []*transaction_items.TransactionItems
// 		expected    map[string]*transaction_items.TransactionItems
// 		errorString string
// 	}{
// 		{
// 			name:     "T2 - Filter transactions by year between 2024-2025",
// 			taskType: models.T2,
// 			payload:  mock.MockTransactionItems,
// 			expected: mock.MockTransactionItemsOutputT2,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			result, err := th.HandleTask(tt.taskType, tt.payload)

// 			if err != nil {
// 				t.Errorf("Unexpected error: %v", err)
// 				return
// 			}

// 			resultSlice := result.([]*transaction_items.TransactionItems)
// 			for _, actual := range resultSlice {
// 				exp, exists := tt.expected[actual.TransactionId]
// 				assert.Equal(t, true, exists, "TransactionId %s not found in expected results", actual.TransactionId)
// 				if exists {
// 					assert.Equal(t, exp, actual, "Transaction item data does not match for TransactionId %s", actual.TransactionId)
// 				}
// 			}
// 		})
// 	}
// }
