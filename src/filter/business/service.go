package business

import (
	"coffee-analisis/src/common/models"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// Generic constraint that ensures
// the type implements our required methods
type TransactionCommon interface {
	GetCreatedAt() time.Time
}

type FilterService struct{}

func NewFilterService() *FilterService {
	return &FilterService{}
}

// Generic filter methods that preserve the original type
func FilterByYearBetween[T TransactionCommon](from, to int, transactions []T) []T {
	log.Debug("Task being processed... | Filtering by year")

	var filtered []T
	for _, transaction := range transactions {
		year := transaction.GetCreatedAt().Year()
		if year >= from && year <= to {
			filtered = append(filtered, transaction)
		}
	}
	return filtered
}

func FilterByHourBetween[T TransactionCommon](from, to int, transactions []T) []T {
	log.Debug("Task being processed... | Filtering by hour")

	var filtered []T
	for _, transaction := range transactions {
		hour := transaction.GetCreatedAt().Hour()
		if hour >= from && hour <= to {
			filtered = append(filtered, transaction)
		}
	}
	return filtered
}

func FilterByTotalAmountGreaterThan(totalAmount float64, transactions []models.Transaction) []models.Transaction {
	log.Debug("Task being processed... | Filtering by amount")

	var filtered []models.Transaction
	for _, transaction := range transactions {
		if transaction.GetFinalAmount() >= totalAmount {
			filtered = append(filtered, transaction)
		}
	}
	return filtered
}
