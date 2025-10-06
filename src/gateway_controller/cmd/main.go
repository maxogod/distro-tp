package main

import (
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/gateway_controller/config"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/server"
)

var log = logger.GetLogger()

func main() {
	time.Sleep(10 * time.Second) // wait for rabbitmq to be ready

	conf, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	log.Infoln("Controller server starting")
	server := server.NewServer(conf)
	err = server.Run()
	if err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}

	log.Infoln("Geodude finished successfully!")
}
