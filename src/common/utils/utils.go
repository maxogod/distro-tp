package utils

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/maxogod/distro-tp/src/common/models/data_batch"
	"github.com/maxogod/distro-tp/src/common/models/raw"
	"google.golang.org/protobuf/proto"
)

func AppendToCSVFile(path string, payload []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	f, openErr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if openErr != nil {
		return openErr
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("error closing file: %v\n", err)
		}
	}(f)

	if _, err := f.Write(payload); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func GetDataBatch(msg []byte) (*data_batch.DataBatch, error) {

	dataBatch := &data_batch.DataBatch{}
	err := proto.Unmarshal(msg, dataBatch)
	if err != nil {
		return nil, err
	}

	return dataBatch, nil
}

func GetTransactions(payload []byte) ([]*raw.Transaction, error) {

	transactions := &raw.TransactionBatch{}
	err := proto.Unmarshal(payload, transactions)
	if err != nil {
		return nil, err
	}

	return transactions.GetTransactions(), nil
}

func GetTransactionItems(payload []byte) ([]*raw.TransactionItems, error) {

	transactions := &raw.TransactionItemsBatch{}
	err := proto.Unmarshal(payload, transactions)
	if err != nil {
		return nil, err
	}

	return transactions.GetTransactionItems(), nil
}

// UnmarshalPayload unmarshals payload into a proto.Message of type T.
func UnmarshalPayload[T proto.Message](payload []byte, msg T) (T, error) {
	err := proto.Unmarshal(payload, msg)
	if err != nil {
		return msg, err
	}
	return msg, nil
}
