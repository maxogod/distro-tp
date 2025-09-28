package client

import (
	"encoding/csv"
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
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
	log.Infoln("[Client Gateway] started")

	csv_file_path := c.conf.DataPath + "/menu_items/menu_items.csv"
	file, err := os.Open(csv_file_path)
	if err != nil {
		log.Errorln("[Client Gateway] failed to open menu items file:", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Errorln("[Client Gateway] failed to read menu items file:", err)
	}

	for _, record := range records {
		log.Infoln("[Client Gateway] menu item:", record)
	}

	return nil
}
