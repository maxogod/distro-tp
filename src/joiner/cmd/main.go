package main

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/joiner/internal/server"
	"github.com/maxogod/distro-tp/src/worker_base/config"
)

var log = logger.GetLogger()

func main() {

	initConfig, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	log.Debugln(initConfig)

	server := server.InitServer(initConfig)

	err = server.Run()
	if err != nil {
		return
	}

}
