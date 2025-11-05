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

func TestSaveIndexedAndReadProtoData(t *testing.T) {
	fh := NewFileHandler()

	path := tmpFilePath(t)

	dataKey := "data@1"

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

	assert.Len(t, total, 1, "Number of transactions read back mismatch")

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
	fh := NewFileHandler()

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

	assert.Len(t, total, 2, "Number of transactions read back mismatch")

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
