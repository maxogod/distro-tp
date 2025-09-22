package main

import (
	"coffee-analisis/src/worker_base/config"
	"coffee-analisis/src/worker_base/internal/server"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {

	cfg, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := config.InitLogger(cfg.LogLevel); err != nil {
		log.Fatalf("%s", err)
	}

	log.Debug(cfg)

	server := server.InitServer(cfg)

	server.Run()

}
