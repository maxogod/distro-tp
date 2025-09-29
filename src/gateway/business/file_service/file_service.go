package file_service

import (
	"encoding/csv"
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/menu_items"
	"github.com/maxogod/distro-tp/src/common/models/store"
	"github.com/maxogod/distro-tp/src/common/models/transaction"
	"github.com/maxogod/distro-tp/src/common/models/transaction_items"
	"github.com/maxogod/distro-tp/src/common/models/user"
	"github.com/maxogod/distro-tp/src/common/utils"
)

var log = logger.GetLogger()

type fileService struct {
	batchSize int
}

func NewFileService(batchSize int) FileService {
	return &fileService{
		batchSize: batchSize,
	}
}

func (fs *fileService) ReadTransactions(path string, batches_ch chan []*transaction.Transaction) {
	log.Debugln("Reading transactions from file:", path)

	defer close(batches_ch)
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return
	}

	batch := make([]*transaction.Transaction, 0, fs.batchSize)
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		t := &transaction.Transaction{
			TransactionId:   record[0],
			StoreId:         int64(utils.ParseIntOrDefault(record[1])),
			PaymentMethod:   int32(utils.ParseIntOrDefault(record[2])),
			VoucherId:       int64(utils.ParseIntOrDefault(record[3])),
			UserId:          int64(utils.ParseIntOrDefault(record[4])),
			OriginalAmount:  utils.ParseFloatOrDefault(record[5]),
			DiscountApplied: utils.ParseFloatOrDefault(record[6]),
			FinalAmount:     utils.ParseFloatOrDefault(record[7]),
			CreatedAt:       record[8],
		}

		batch = append(batch, t)

		if len(batch) >= fs.batchSize {
			batches_ch <- batch
			batch = make([]*transaction.Transaction, 0, fs.batchSize)
		}
	}

	if len(batch) > 0 {
		batches_ch <- batch
	}
}

func (fs *fileService) ReadTransactionItems(path string) (chan []transaction_items.TransactionItems, error) {
	return nil, nil
}

func (fs *fileService) ReadMenuItems(path string) (chan []menu_items.MenuItem, error) {
	return nil, nil
}

func (fs *fileService) ReadStores(path string) (chan []store.Store, error) {
	return nil, nil
}

func (fs *fileService) ReadUsers(path string) (chan []user.User, error) {
	return nil, nil
}
