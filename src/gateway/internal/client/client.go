package client

import (
	"fmt"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/common/models/transaction"
	"github.com/maxogod/distro-tp/src/common/network"
	"github.com/maxogod/distro-tp/src/gateway/business/file_service"
	"github.com/maxogod/distro-tp/src/gateway/config"
	"github.com/maxogod/distro-tp/src/gateway/internal/utils"
	"google.golang.org/protobuf/proto"
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

	conn := network.NewConnectionInterface()
	conn.Connect(fmt.Sprintf("%s:%d", c.conf.ServerHost, c.conf.ServerPort))
	defer conn.Close()

	ch := make(chan []*transaction.Transaction)
	go fs.ReadAsBatches(csv_file_path, ch, utils.TransactionFromRecord)

	for batch := range ch {
		tBatch := transaction.TransactionBatch{
			TaskType:     1,
			Transactions: batch,
		}
		data, err := proto.Marshal(&tBatch)
		if err != nil {
			log.Errorf("failed to marshal transaction batch: %v", err)
			continue
		}

		err = conn.SendData(data)
		if err != nil {
			log.Errorf("failed to send transaction batch: %v", err)
		}
	}

	return nil
}
