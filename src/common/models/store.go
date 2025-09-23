package models

type Store struct {
	StoreID    int
	StoreName  string
	Street     string
	PostalCode int
	City       string
	State      string
	Latitude   float64
	Longitude  float64
}
