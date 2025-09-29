package client

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/transaction"
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
	fs := file_service.NewFileService(100)

	ch := make(chan []*transaction.Transaction)
	go fs.ReadTransactions(csv_file_path, ch)

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
