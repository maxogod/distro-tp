package client

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/transaction"
	"github.com/maxogod/distro-tp/src/common/utils"
	"github.com/maxogod/distro-tp/src/gateway/business/file_service"
	"github.com/maxogod/distro-tp/src/gateway/config"
)

var log = logger.GetLogger()

type client struct {
	conf *config.Config
}

func NewClient(conf *config.Config) Client {
	return &client{
		conf: conf,
	}
}

func (c *client) Start() error {
	log.Infoln("started")

	csv_file_path := c.conf.DataPath + "/transactions/transactions_202407.csv"
	fs := file_service.NewFileService[*transaction.Transaction](100)

	ch := make(chan []*transaction.Transaction)
	go fs.ReadAsBatches(csv_file_path, ch, func(record []string) *transaction.Transaction {
		return &transaction.Transaction{
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
	})

	for batch := range ch {
		log.Infof("received batch of %d transactions", len(batch))
		for _, t := range batch {
			log.Infof("transaction: %s - %s - %f", t.TransactionId, t.CreatedAt, t.FinalAmount)
			break
		}
		break
	}

	return nil
}
