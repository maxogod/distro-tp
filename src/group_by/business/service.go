package business

import (
	"fmt"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/raw"
)

var log = logger.GetLogger()

type GroupByService struct{}

func NewGroupByService() *GroupByService {
	return &GroupByService{}
}

func (s *GroupByService) GroupTransactionByYearMonth(transactions []*raw.TransactionItems) map[string][]*raw.TransactionItems {
	result := make(map[string][]*raw.TransactionItems)

	for _, tx := range transactions {
		createdAt := tx.GetCreatedAt()
		// Parse the date, assuming format "2006-01-02 15:04:05" or ISO 8601
		t, err := time.Parse("2006-01-02 15:04:05", createdAt)
		if err != nil {
			log.Debugf("Failed to parse date %s: %v", createdAt, err)
			continue
		}
		key := fmt.Sprintf("%04d-%02d", t.Year(), t.Month())
		result[key] = append(result[key], tx)
	}

	return result
}

func (s *GroupByService) GroupItemsBySemester(items []*raw.Transaction) map[string][]*raw.Transaction {
	result := make(map[string][]*raw.Transaction)

	for _, item := range items {
		createdAt := item.GetCreatedAt()
		t, err := time.Parse("2006-01-02 15:04:05", createdAt)
		if err != nil {
			log.Debugf("Failed to parse date %s: %v", createdAt, err)
			continue
		}

		// Determine semester: H1 (Jan-Jun), H2 (Jul-Dec)
		semester := "H1"
		if t.Month() >= 7 {
			semester = "H2"
		}

		key := fmt.Sprintf("%04d_%s", t.Year(), semester)
		item.CreatedAt = key // Update CreatedAt to reflect the semester grouping
		result[key] = append(result[key], item)
	}

	return result
}

func (s *GroupByService) GroupTransactionByUserID(transactions []*raw.Transaction) map[string][]*raw.Transaction {
	result := make(map[string][]*raw.Transaction)

	for _, tx := range transactions {
		userID := fmt.Sprintf("%d", tx.GetUserId())
		result[userID] = append(result[userID], tx)
	}

	return result
}
