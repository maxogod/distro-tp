package client

import (
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

	return nil
}
