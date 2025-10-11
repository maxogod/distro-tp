package business

import (
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

var log = logger.GetLogger()

const timeFormat = "2006-01-02 15:04:05" // Example format

type filterService struct{}

func NewFilterService() FilterService {
	return &filterService{}
}

func (f *filterService) FilterByYear(batch *raw.TransactionBatch) error {
	filtered := make([]*raw.Transaction, 0, len(batch.Transactions))
	for _, tx := range batch.Transactions {
		t, err := time.Parse(timeFormat, tx.CreatedAt)
		if err != nil {
			log.Warnf("Skipping transaction %s due to parse error: %v", tx.TransactionId, err)
			continue // skip invalid
		}
		year := t.Year()
		if year >= MinYear && year <= MaxYear {
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
			log.Warnf("Skipping transaction %s due to parse error: %v", tx.ItemId, err)
			continue // skip invalid
		}
		year := t.Year()
		if year >= MinYear && year <= MaxYear {
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
		if (hour >= MinHour && hour < MaxHour) || (hour == MaxHour && minute == 0) {
			filtered = append(filtered, tx)
		}
	}
	batch.Transactions = filtered
	return nil
}

func (f *filterService) FilterByFinalAmount(batch *raw.TransactionBatch) error {
	filtered := make([]*raw.Transaction, 0, len(batch.Transactions))
	for _, tx := range batch.Transactions {
		if tx.FinalAmount >= MinFinalAmount {
			filtered = append(filtered, tx)
		}
	}
	batch.Transactions = filtered
	return nil
}
