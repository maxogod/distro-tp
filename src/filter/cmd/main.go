package main

import (
	"coffee-analisis/src/filter/config"
	"coffee-analisis/src/filter/internal/server"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {

	conf, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := config.InitLogger(conf.LogLevel); err != nil {
		log.Fatalf("%s", err)
	}

	log.Debug(conf)

	server := server.InitServer(conf)

	server.Run()

}
