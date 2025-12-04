package reducer_test

import (
	"github.com/maxogod/distro-tp/src/common/models/reduced"
)

var ReducedTransactionMock2 = reduced.TotalSumItem{
	ItemId:    "item1",
	YearMonth: "2025-07",
	Quantity:  4,
	Subtotal:  400.0,
}

var ReducedTransactionMock3 = reduced.TotalPaymentValue{
	StoreId:     "store1",
	Semester:    "2025-H2",
	FinalAmount: 200.0,
}

var ReducedTransactionMock4 = reduced.CountedUserTransactions{
	StoreId:             "store3",
	UserId:              "user1",
	TransactionQuantity: 2,
}
