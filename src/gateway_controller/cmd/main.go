package main

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/gateway_controller/config"
	"github.com/maxogod/distro-tp/src/gateway_controller/internal/server"
)

var log = logger.GetLogger()

func main() {

	conf, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	server := server.InitServer(conf)

	err = server.Run()
	if err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
	log.Debug("Geodude thanks you for using the Gateway Controller!")
}
