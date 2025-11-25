package aggregator_test

import (
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var MockTpvOutput = reduced.TotalPaymentValueBatch{
	TotalPaymentValues: []*reduced.TotalPaymentValue{
		{
			StoreId:     "1",
			Semester:    "2024-H1",
			FinalAmount: 600.0,
		},
		{
			StoreId:     "2",
			Semester:    "2025-H1",
			FinalAmount: 100.0,
		},
	},
}

var MockUsersDupQuantitiesOutput = map[string](map[int32]int){
	"1": {
		200: 1,
		100: 2,
		50:  1,
	},
	"2": {
		10: 1,
	},
}
