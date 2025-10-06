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

// This represents T2
func (s *GroupByService) GroupItemsByYearMonthAndItem(transactions []*raw.TransactionItems) map[string][]*raw.TransactionItems {
	result := make(map[string][]*raw.TransactionItems)

	for _, tx := range transactions {
		createdAt := tx.GetCreatedAt()
		itemID := tx.GetItemId()
		t, err := time.Parse("2006-01-02 15:04:05", createdAt)
		if err != nil {
			log.Debugf("Failed to parse date %s: %v", createdAt, err)
			continue
		}
		year_month := fmt.Sprintf("%04d-%02d", t.Year(), t.Month())
		tx.CreatedAt = year_month // change createdAt to year-month format

		key := fmt.Sprintf("%s-%d", year_month, itemID)
		result[key] = append(result[key], tx)
	}

	return result
}

// This represents T3
func (s *GroupByService) GroupItemsBySemesterAndStore(items []*raw.Transaction) map[string][]*raw.Transaction {
	result := make(map[string][]*raw.Transaction)

	for _, item := range items {
		storeID := item.GetStoreId()
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
		semesterDate := fmt.Sprintf("%04d-%s", t.Year(), semester)
		item.CreatedAt = semesterDate // change createdAt to semester format

		key := fmt.Sprintf("%s_%d", semesterDate, storeID)
		result[key] = append(result[key], item)
	}

	return result
}

// This represents T4
func (s *GroupByService) GroupTransactionByUserAndStore(transactions []*raw.Transaction) map[string][]*raw.Transaction {
	result := make(map[string][]*raw.Transaction)

	for _, tx := range transactions {
		userID := fmt.Sprintf("%d", tx.GetUserId())
		storeID := tx.GetStoreId()
		key := fmt.Sprintf("%s_%d", userID, storeID)
		result[key] = append(result[key], tx)
	}

	return result
}
