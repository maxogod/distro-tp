package reducer_test

import (
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var ReducedTransactionMock1 = reduced.TotalPaymentValue{
	StoreId:     "store1",
	Semester:    "2025-H2",
	FinalAmount: 200.0,
}

var ReducedTransactionMock2_1 = reduced.TotalProfitBySubtotal{
	ItemId:    "item1",
	YearMonth: "2025-07",
	Subtotal:  400,
}

var ReducedTransactionMock2_2 = reduced.TotalSoldByQuantity{
	ItemId:    "item1",
	YearMonth: "2025-07",
	Quantity:  4,
}
