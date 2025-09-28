package main

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/gateway/config"
	"github.com/maxogod/distro-tp/src/gateway/internal/client"
)

var log = logger.GetLogger()

func main() {
	log.Debugln("[Client Gateway] initializing")

	conf, err := config.InitConfig()
	if err != nil {
		log.Fatalln("[Client Gateway] failed to initialize config:", err)
	}

	c := client.NewClient(conf)
	if err := c.Start(); err != nil {
		log.Fatalln("[Client Gateway] failed to start client:", err)
	}
}
