package business_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/group_by/business"
	"github.com/stretchr/testify/assert"
)

func TestGroupTransactionByYearMonth(t *testing.T) {
	svc := business.NewGroupByService()
	items := []*raw.TransactionItems{
		&raw.TransactionItems{CreatedAt: "2025-01-15 10:00:00"},
		&raw.TransactionItems{CreatedAt: "2025-01-20 12:00:00"},
		&raw.TransactionItems{CreatedAt: "2025-07-05 09:00:00"},
	}
	result := svc.GroupTransactionByYearMonth(items)
	assert.Len(t, result["2025-01"], 2)
	assert.Len(t, result["2025-07"], 1)
}

func TestGroupItemsBySemester(t *testing.T) {
	svc := business.NewGroupByService()
	transactions := []*raw.Transaction{
		&raw.Transaction{CreatedAt: "2025-03-10 08:00:00"},
		&raw.Transaction{CreatedAt: "2025-08-15 14:00:00"},
		&raw.Transaction{CreatedAt: "2025-06-01 11:00:00"},
	}
	result := svc.GroupItemsBySemester(transactions)
	assert.Len(t, result["2025_H1"], 2)
	assert.Len(t, result["2025_H2"], 1)
}

func TestGroupTransactionByUserID(t *testing.T) {
	svc := business.NewGroupByService()
	transactions := []*raw.Transaction{
		&raw.Transaction{UserId: 101},
		&raw.Transaction{UserId: 102},
		&raw.Transaction{UserId: 101},
	}
	result := svc.GroupTransactionByUserID(transactions)
	assert.Len(t, result["101"], 2)
	assert.Len(t, result["102"], 1)
}
