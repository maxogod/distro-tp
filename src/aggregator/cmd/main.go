package main

import (
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/internal/server"
	"github.com/maxogod/distro-tp/src/common/logger"
)

var log = logger.GetLogger()

func main() {

	conf, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	server := server.InitServer(conf)

	server.Run()

}
