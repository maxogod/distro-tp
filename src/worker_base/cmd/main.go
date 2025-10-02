package main

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/worker_base/config"
	"github.com/maxogod/distro-tp/src/worker_base/internal/server"
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
