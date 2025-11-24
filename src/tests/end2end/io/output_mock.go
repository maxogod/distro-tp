package mock

import (
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var MockTransactionsOutput = []*raw.Transaction{
	{
		TransactionId: "1", // Good
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   150.0,
		CreatedAt:     "2025-07-01 06:01:00",
	},
	{
		TransactionId: "2", // Good
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   75.0,
		CreatedAt:     "2025-07-01 06:00:00",
	},
	{
		TransactionId: "3", // Good
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 23:00:00",
	},
}

var MockTPVOutput = map[string]*reduced.TotalPaymentValue{
	"Store Two": {
		StoreId:     "Store Two",
		Semester:    "2024-H1",
		FinalAmount: 600.0,
	},
	"Store One": {
		StoreId:     "Store One",
		Semester:    "2025-H1",
		FinalAmount: 600.0,
	},
}
