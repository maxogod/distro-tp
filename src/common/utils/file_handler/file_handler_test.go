package file_handler

import (
	"path/filepath"
	"testing"

	"github.com/maxogod/distro-tp/src/common/models/raw"
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
	fh := NewFileHandler(5)

	path := tmpFilePath(t)
	ch := make(chan proto.Message)

	go func() {
		err := fh.SaveProtoData(path, ch)
		if err != nil {
			t.Errorf("SaveProtoData error: %v", err)
		}
	}()

	// send some dummy messages
	protoMessages := make([]proto.Message, len(Transactions))
	for i := range Transactions {
		protoMessages[i] = Transactions[i]
	}
	for _, msg := range protoMessages {
		ch <- msg
	}
	close(ch)

	// read them back
	readCh := make(chan [][]byte)
	go fh.ReadAsBatches(path, readCh)

	var total []*raw.Transaction
	for batch := range readCh {
		for _, msg := range batch {
			var tr raw.Transaction
			if err := proto.Unmarshal(msg, &tr); err != nil {
				t.Errorf("proto unmarshal error: %v", err)
				continue
			}
			total = append(total, &tr)
		}
	}

	if len(total) != len(Transactions) {
		t.Fatalf("expected %d transactions, got %d", len(Transactions), len(total))
	}

	expected := make(map[string]*raw.Transaction, len(Transactions))
	for _, tr := range Transactions {
		expected[tr.TransactionId] = tr
	}

	var missing []string
	for _, tr := range total {
		exp, ok := expected[tr.TransactionId]
		if !ok {
			t.Errorf("unexpected transaction id %s", tr.TransactionId)
			continue
		}
		if tr.StoreId != exp.StoreId || tr.UserId != exp.UserId || tr.FinalAmount != exp.FinalAmount || tr.CreatedAt != exp.CreatedAt {
			t.Errorf("transaction %s mismatch: expected %+v, got %+v", tr.TransactionId, exp, tr)
		}
		delete(expected, tr.TransactionId)
	}

	for id := range expected {
		missing = append(missing, id)
	}
	if len(missing) > 0 {
		t.Errorf("missing transactions: %v", missing)
	}

	fh.Close()

}

func TestSaveIndexedAndReadProtoData(t *testing.T) {
	fh := NewFileHandler(5)

	path := tmpFilePath(t)

	dataKey := "data-1"

	for _, tr := range Transactions {

		updateFunc := func(protoBytes *[]byte) {
			if len(*protoBytes) == 0 {
				data, _ := proto.Marshal(tr)
				*protoBytes = data
				return
			}
			existing := &raw.Transaction{}
			if err := proto.Unmarshal(*protoBytes, existing); err != nil {
				t.Fatalf("proto unmarshal error in updateFunc: %v", err)
			}
			existing.FinalAmount += tr.FinalAmount
			updatedBytes, _ := proto.Marshal(existing)
			*protoBytes = updatedBytes
		}

		err := fh.SaveIndexedData(path, dataKey, updateFunc)
		if err != nil {
			t.Fatalf("SaveIndexedData error: %v", err)
		}

	}

	// read them back
	readCh := make(chan [][]byte)
	go fh.ReadAsBatches(path, readCh)

	var total []*raw.Transaction
	for batch := range readCh {
		for _, msg := range batch {
			var tr raw.Transaction
			if err := proto.Unmarshal(msg, &tr); err != nil {
				t.Errorf("proto unmarshal error: %v", err)
				continue
			}
			total = append(total, &tr)
		}
	}

	if len(total) != 1 {
		t.Fatalf("expected %d transaction, got %d", 1, len(total))
	}

	expectedFinalAmount := 0.0
	for _, tr := range Transactions {
		expectedFinalAmount += tr.FinalAmount
	}

	if total[0].FinalAmount != expectedFinalAmount {
		t.Errorf("expected final amount %f, got %f", expectedFinalAmount, total[0].FinalAmount)
	}

	fh.Close()
}

func TestSaveIndexedWithVariedData(t *testing.T) {
	fh := NewFileHandler(5)

	path := tmpFilePath(t)

	for _, tr := range Transactions {

		updateFunc := func(protoBytes *[]byte) {
			if len(*protoBytes) == 0 {
				data, _ := proto.Marshal(tr)
				*protoBytes = data
				return
			}
			existing := &raw.Transaction{}
			if err := proto.Unmarshal(*protoBytes, existing); err != nil {
				t.Fatalf("proto unmarshal error in updateFunc: %v", err)
			}
			existing.FinalAmount += tr.FinalAmount
			updatedBytes, _ := proto.Marshal(existing)
			*protoBytes = updatedBytes
		}
		dataKey := string(tr.TransactionId)
		err := fh.SaveIndexedData(path, dataKey, updateFunc)
		if err != nil {
			t.Fatalf("SaveIndexedData error: %v", err)
		}

	}

	// read them back
	readCh := make(chan [][]byte)
	go fh.ReadAsBatches(path, readCh)

	var total []*raw.Transaction
	for batch := range readCh {
		for _, msg := range batch {
			var tr raw.Transaction
			if err := proto.Unmarshal(msg, &tr); err != nil {
				t.Errorf("proto unmarshal error: %v", err)
				continue
			}
			total = append(total, &tr)
		}
	}

	if len(total) != 2 {
		t.Fatalf("expected %d transaction, got %d", 2, len(total))
	}

	mapFinalAmounts := make(map[string]float64)
	for _, tr := range total {
		mapFinalAmounts[tr.TransactionId] = tr.FinalAmount
	}

	expectedSums := make(map[string]float64)
	for _, tr := range Transactions {
		expectedSums[tr.TransactionId] += tr.FinalAmount
	}

	for id, expectedSum := range expectedSums {
		if mapFinalAmounts[id] != expectedSum {
			t.Errorf("for transaction id %s, expected final amount %f, got %f", id, expectedSum, mapFinalAmounts[id])
		}
	}

	fh.Close()
}
