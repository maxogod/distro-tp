package protocol

import "github.com/maxogod/distro-tp/src/common/models/transaction"

type protocol struct {
}

func NewProtocol() Protocol {
	return &protocol{}
}

func (p *protocol) GetTransactionBatch() (*transaction.TransactionBatch, error) {
	return nil, nil
}

func (p *protocol) SendTransactionBatch(t *transaction.TransactionBatch) error {
	return nil
}
