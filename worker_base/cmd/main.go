package main

import (
	"coffee-analisis/worker_base/config"
	"coffee-analisis/worker_base/internal/server"
	"os"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {

	cfg, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
		os.Exit(1)
	}

	if err := config.InitLogger(cfg.LogLevel); err != nil {
		log.Fatalf("%s", err)
		os.Exit(1)
	}

	log.Debug(cfg)

	server := server.InitServer(cfg)

	server.Run()

}
