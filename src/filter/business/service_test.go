package business_test

import (
	"coffee-analisis/src/common/models"
	"coffee-analisis/src/filter/business"
	"testing"
	"time"
)

func TestFilterByYearBetween(t *testing.T) {
	tests := []struct {
		name             string
		from, to         int
		transactions     []models.Transaction
		transactionItems []models.TransactionItem
		wantCount        int
	}{
		{
			name: "include transactions between 2023 and 2024",
			from: 2023, to: 2024,
			transactions: []models.Transaction{
				{CreatedAt: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)},
				{CreatedAt: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)},
				{CreatedAt: time.Date(2022, 7, 1, 0, 0, 0, 0, time.UTC)},
			},
			wantCount: 2,
		},
		{
			name: "include transaction items between 2023 and 2024",
			from: 2023, to: 2024,
			transactionItems: []models.TransactionItem{
				{CreatedAt: time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC)},
				{CreatedAt: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)},
				{CreatedAt: time.Date(2022, 7, 1, 0, 0, 0, 0, time.UTC)},
			},
			wantCount: 2,
		},
		{
			name: "no matches",
			from: 2010, to: 2015,
			transactions: []models.Transaction{
				{CreatedAt: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)},
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
		transactions []models.TransactionItem
		wantCount    int
	}{
		{
			name: "hours between 10 and 13",
			from: 10, to: 13,
			transactions: []models.TransactionItem{
				{CreatedAt: time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)},
				{CreatedAt: time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)},
				{CreatedAt: time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC)},
			},
			wantCount: 1,
		},
		{
			name: "hours between 10 and 13 with minutes included",
			from: 10, to: 13,
			transactions: []models.TransactionItem{
				{CreatedAt: time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)},
				{CreatedAt: time.Date(2024, 1, 1, 11, 30, 0, 0, time.UTC)},
				{CreatedAt: time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC)},
			},
			wantCount: 1,
		},
		{
			name: "all within range",
			from: 8, to: 16,
			transactions: []models.TransactionItem{
				{CreatedAt: time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)},
				{CreatedAt: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
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
		transactions []models.Transaction
		wantCount    int
	}{
		{
			name:      "filter above 100",
			threshold: 100,
			transactions: []models.Transaction{
				{FinalAmount: 50},
				{FinalAmount: 150},
				{FinalAmount: 200},
			},
			wantCount: 2,
		},
		{
			name:      "filter above 250",
			threshold: 250,
			transactions: []models.Transaction{
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
