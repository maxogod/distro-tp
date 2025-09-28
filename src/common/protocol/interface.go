package protocol

import "github.com/maxogod/distro-tp/src/common/models/transaction"

type Protocol interface {
	GetTransactionBatch() (*transaction.TransactionBatch, error)
	SendTransactionBatch(t *transaction.TransactionBatch) error
}
