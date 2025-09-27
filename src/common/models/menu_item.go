package models

import "time"

type MenuItem struct {
	ItemId        int
	ItemName      string
	Category      string
	Price         float64
	IsSeasonal    bool
	AvailableFrom time.Time
	AvailableTo   time.Time
}
