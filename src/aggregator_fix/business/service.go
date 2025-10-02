package business

import (
	"time"

	"github.com/maxogod/distro-tp/src/common/models/raw"
)

// Generic constraint that ensures
// the type implements our required methods
type TransactionCommon interface {
	GetCreatedAt() string
}

type AggregatorService struct{}

func NewAggregatorService() *AggregatorService {
	return &AggregatorService{}
}

// TODO CHANGE WITH AGGREGATE LOGIC HERE
func FilterByYearBetween[T TransactionCommon](from, to int, transactions []T) []T {

	var filtered []T
	for _, transaction := range transactions {

		date, err := time.Parse("2006-01-02 15:04:05", transaction.GetCreatedAt())
		if err != nil {
			continue
		}

		year := date.Year()
		if year >= from && year <= to {
			filtered = append(filtered, transaction)
		}
	}
	return filtered
}

func FilterByHourBetween[T TransactionCommon](from, to int, transactions []T) []T {
	var filtered []T
	for _, transaction := range transactions {
		date, err := time.Parse("2006-01-02 15:04:05", transaction.GetCreatedAt())
		if err != nil {
			continue
		}

		// Extract hour and minute as decimal (e.g., 11:45 = 11.75)
		timeDecimal := float64(date.Hour()) + float64(date.Minute())/60.0

		if timeDecimal >= float64(from) && timeDecimal < float64(to+1) {
			filtered = append(filtered, transaction)
		}
	}
	return filtered
}

func FilterByTotalAmountGreaterThan(totalAmount float64, transactions []*raw.Transaction) []*raw.Transaction {
	var filtered []*raw.Transaction
	for _, transaction := range transactions {
		if transaction.GetFinalAmount() >= totalAmount {
			filtered = append(filtered, transaction)
		}
	}
	return filtered
}
