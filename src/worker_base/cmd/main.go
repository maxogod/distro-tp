package main

import (
	"github.com/maxogod/distro-tp/common/logger"
	"github.com/maxogod/distro-tp/worker_base/config"
	"github.com/maxogod/distro-tp/worker_base/internal/server"
)

var log = logger.GetLogger()

func main() {

	cfg, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := config.InitLogger(cfg.LogLevel); err != nil {
		log.Fatalf("%s", err)
	}

	log.Debugln(cfg)

	server := server.InitServer(cfg)

	server.Run()

}
