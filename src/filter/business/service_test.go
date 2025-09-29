package business_test

import (
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/transaction"
	"github.com/maxogod/distro-tp/src/common/models/transaction_items"
	"github.com/maxogod/distro-tp/src/filter/business"
)

func TestFilterByYearBetween(t *testing.T) {
	tests := []struct {
		name             string
		from, to         int
		transactions     []*transaction.Transaction
		transactionItems []*transaction_items.TransactionItems
		wantCount        int
	}{
		{
			name: "include transactions between 2023 and 2024",
			from: 2023, to: 2024,
			transactions: []*transaction.Transaction{
				{CreatedAt: "2023-05-01 00:00:00"},
				{CreatedAt: "2024-06-01 00:00:00"},
				{CreatedAt: "2022-07-01 00:00:00"},
			},
			wantCount: 2,
		},
		{
			name: "include transaction items between 2023 and 2024",
			from: 2023, to: 2024,
			transactionItems: []*transaction_items.TransactionItems{
				{CreatedAt: "2023-05-01 00:00:00"},
				{CreatedAt: "2024-06-01 00:00:00"},
				{CreatedAt: "2022-07-01 00:00:00"},
			},
			wantCount: 2,
		},
		{
			name: "no matches",
			from: 2010, to: 2015,
			transactions: []*transaction.Transaction{
				{CreatedAt: "2023-01-01 00:00:00"},
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotCount int

			if len(tt.transactions) > 0 {
				got := business.FilterByYearBetween(tt.from, tt.to, tt.transactions)
				gotCount = len(got)
			} else if len(tt.transactionItems) > 0 {
				got := business.FilterByYearBetween(tt.from, tt.to, tt.transactionItems)
				gotCount = len(got)
			}

			if gotCount != tt.wantCount {
				t.Errorf("%s: Expected %d, got %d", tt.name, tt.wantCount, gotCount)
			}
		})
	}

}

func TestFilterByHourBetween(t *testing.T) {
	tests := []struct {
		name         string
		from, to     int
		transactions []*transaction_items.TransactionItems
		wantCount    int
	}{
		{
			name: "hours between 10 and 13",
			from: 10, to: 13,
			transactions: []*transaction_items.TransactionItems{
				{CreatedAt: "2024-01-01 09:00:00"},
				{CreatedAt: "2024-01-01 11:00:00"},
				{CreatedAt: "2024-01-01 15:00:00"},
			},
			wantCount: 1,
		},
		{
			name: "hours between 10 and 13 with minutes included",
			from: 10, to: 13,
			transactions: []*transaction_items.TransactionItems{
				{CreatedAt: "2024-01-01 09:00:00"},
				{CreatedAt: "2024-01-01 11:30:00"},
				{CreatedAt: "2024-01-01 15:00:00"},
			},
			wantCount: 1,
		},
		{
			name: "all within range",
			from: 8, to: 16,
			transactions: []*transaction_items.TransactionItems{
				{CreatedAt: "2024-01-01 09:00:00"},
				{CreatedAt: "2024-01-01 12:00:00"},
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := business.FilterByHourBetween(tt.from, tt.to, tt.transactions)
			if len(got) != tt.wantCount {
				t.Errorf("%s: Expected %d, got %d", tt.name, tt.wantCount, len(got))
			}
		})
	}
}

func TestFilterByTotalAmountGreaterThan(t *testing.T) {
	tests := []struct {
		name         string
		threshold    float64
		transactions []*transaction.Transaction
		wantCount    int
	}{
		{
			name:      "filter above 100",
			threshold: 100,
			transactions: []*transaction.Transaction{
				{FinalAmount: 50},
				{FinalAmount: 150},
				{FinalAmount: 200},
			},
			wantCount: 2,
		},
		{
			name:      "filter above 250",
			threshold: 250,
			transactions: []*transaction.Transaction{
				{FinalAmount: 100},
				{FinalAmount: 200},
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := business.FilterByTotalAmountGreaterThan(tt.threshold, tt.transactions)
			if len(got) != tt.wantCount {
				t.Errorf("%s: Expected %d, got %d", tt.name, tt.wantCount, len(got))
			}
		})
	}
}
