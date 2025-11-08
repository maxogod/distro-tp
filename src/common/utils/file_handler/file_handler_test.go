package file_handler

import (
	"path/filepath"
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/raw"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var Transactions = []*raw.Transaction{
	{
		TransactionId: "1",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 06:01:00",
	},
	{
		TransactionId: "1",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 06:00:00",
	},
	{
		TransactionId: "2",
		StoreId:       "storeID",
		UserId:        "userID",
		FinalAmount:   100.0,
		CreatedAt:     "2025-07-01 23:00:00",
	},
}

// helper: create a temp file path
func tmpFilePath(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "testfile.txt")
}

func TestSaveAndReadProtoData(t *testing.T) {
	fh := NewFileHandler()

	path := tmpFilePath(t)
	ch := make(chan []byte)

	go func() {
		err := fh.SaveData(path, ch)
		if err != nil {
			t.Errorf("SaveData error: %v", err)
		}
	}()

	// send some dummy messages
	for _, tr := range Transactions {
		data, _ := proto.Marshal(tr)
		ch <- data
	}
	close(ch)

	// read them back
	readCh := make(chan []byte)
	go fh.ReadData(path, readCh)

	var total []*raw.Transaction
	for msg := range readCh {
		var tr raw.Transaction
		if err := proto.Unmarshal(msg, &tr); err != nil {
			t.Errorf("proto unmarshal error: %v", err)
			continue
		}
		total = append(total, &tr)
	}

	assert.Len(t, total, len(Transactions), "Number of transactions read back mismatch")

	for i, tr := range Transactions {
		if total[i].TransactionId != tr.TransactionId || total[i].FinalAmount != tr.FinalAmount {
			t.Errorf("expected transaction %v, got %v", tr, total[i])
		}
	}

	fh.Close()

}
