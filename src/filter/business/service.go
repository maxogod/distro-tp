package business

import (
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

const timeFormat = "2006-01-02 15:04:05" // Example format

type filterService struct {
	minYear        int
	maxYear        int
	minFinalAmount float64
	minHour        int
	maxHour        int
}

func NewFilterService(minYear, maxYear, minHour, maxHour int, minFinalAmount float64) FilterService {
	return &filterService{
		minYear:        minYear,
		maxYear:        maxYear,
		minFinalAmount: minFinalAmount,
		minHour:        minHour,
		maxHour:        maxHour,
	}
}

func (f *filterService) FilterByYear(batch *raw.TransactionBatch) error {
	filtered := make([]*raw.Transaction, 0, len(batch.Transactions))
	for _, tx := range batch.Transactions {
		t, err := time.Parse(timeFormat, tx.CreatedAt)
		if err != nil {
			logger.Logger.Warnf("Skipping transaction %s due to parse error: %v", tx.TransactionId, err)
			continue // skip invalid
		}
		year := t.Year()
		if year >= f.minYear && year <= f.maxYear {
			filtered = append(filtered, tx)
		}
	}
	batch.Transactions = filtered
	return nil
}

func (f *filterService) FilterItemsByYear(batch *raw.TransactionItemsBatch) error {
	filtered := make([]*raw.TransactionItem, 0, len(batch.TransactionItems))
	for _, tx := range batch.TransactionItems {
		t, err := time.Parse(timeFormat, tx.CreatedAt)
		if err != nil {
			logger.Logger.Warnf("Skipping transaction %s due to parse error: %v", tx.ItemId, err)
			continue // skip invalid
		}
		year := t.Year()
		if year >= f.minYear && year <= f.maxYear {
			filtered = append(filtered, tx)
		}
	}
	batch.TransactionItems = filtered
	return nil
}

func (f *filterService) FilterByTime(batch *raw.TransactionBatch) error {
	filtered := make([]*raw.Transaction, 0, len(batch.Transactions))
	for _, tx := range batch.Transactions {
		t, err := time.Parse(timeFormat, tx.CreatedAt)
		if err != nil {
			continue // skip invalid
		}
		hour := t.Hour()
		minute := t.Minute()

		// Between Min and Max strictly
		if (hour >= f.minHour && hour < f.maxHour) || (hour == f.maxHour && minute == 0) {
			filtered = append(filtered, tx)
		}
	}
	batch.Transactions = filtered
	return nil
}

func (f *filterService) FilterByFinalAmount(batch *raw.TransactionBatch) error {
	filtered := make([]*raw.Transaction, 0, len(batch.Transactions))
	for _, tx := range batch.Transactions {
		if tx.FinalAmount >= f.minFinalAmount {
			filtered = append(filtered, tx)
		}
	}
	batch.Transactions = filtered
	return nil
}

func (f *filterService) FilterNullUserIDs(batch *raw.TransactionBatch) error {
	filtered := make([]*raw.Transaction, 0, len(batch.Transactions))
	for _, tx := range batch.Transactions {
		if tx.UserId != "" {
			filtered = append(filtered, tx)
		}
	}
	batch.Transactions = filtered
	return nil
}
