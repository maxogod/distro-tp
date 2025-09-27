package business

import (
	"time"

	"github.com/maxogod/distro-tp/src/common/models"
)

// Generic constraint that ensures
// the type implements our required methods
type TransactionCommon interface {
	GetCreatedAt() time.Time
}

type FilterService struct{}

func NewFilterService() *FilterService {
	return &FilterService{}
}

func FilterByYearBetween[T TransactionCommon](from, to int, transactions []T) []T {

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
	var filtered []T
	for _, transaction := range transactions {
		createdAt := transaction.GetCreatedAt()

		// Extract hour and minute as decimal (e.g., 11:45 = 11.75)
		timeDecimal := float64(createdAt.Hour()) + float64(createdAt.Minute())/60.0

		if timeDecimal >= float64(from) && timeDecimal < float64(to+1) {
			filtered = append(filtered, transaction)
		}
	}
	return filtered
}

func FilterByTotalAmountGreaterThan(totalAmount float64, transactions []models.Transaction) []models.Transaction {

	var filtered []models.Transaction
	for _, transaction := range transactions {
		if transaction.GetFinalAmount() >= totalAmount {
			filtered = append(filtered, transaction)
		}
	}
	return filtered
}
