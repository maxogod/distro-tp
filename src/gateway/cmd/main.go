package main

import (
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/gateway/config"
	"github.com/maxogod/distro-tp/src/gateway/internal/client"
)

var log = logger.GetLogger()

func main() {
	log.Debugln("initializing")

	if len(os.Args) < 2 {
		log.Fatalln("no arguments provided")
	}

	conf, err := config.InitConfig()
	if err != nil {
		log.Fatalln("failed to initialize config:", err)
	}

	c := client.NewClient(conf)
	if err := c.Start(os.Args[1]); err != nil {
		log.Fatalln("Client error:", err)
	}
}
