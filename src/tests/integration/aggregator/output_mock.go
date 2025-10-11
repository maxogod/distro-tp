package filter_test

import (
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var MockTpvOutput = []*reduced.TotalPaymentValue{
	{
		StoreId:     "1",
		Semester:    "2025-H2",
		FinalAmount: 200.0,
	},
	{
		StoreId:     "2",
		Semester:    "2025-H1",
		FinalAmount: 100.0,
	},
	{
		StoreId:     "3",
		Semester:    "2025-H1",
		FinalAmount: 10.0,
	},
}
