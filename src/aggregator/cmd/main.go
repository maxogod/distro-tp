package main

import (
	"os"
	"time"

	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/internal/server"
	"github.com/maxogod/distro-tp/src/common/logger"
)

var log = logger.GetLogger()

func main() {

	time.Sleep(12 * time.Second) // Wait for RabbitMQ to be ready

	initConfig, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err := os.MkdirAll(initConfig.StorePath, 0755); err != nil {
		log.Errorf("failed to create output directory: %v", err)
		return
	}

	log.Debugln(initConfig)

	server := server.InitServer(initConfig)

	err = server.Run()
	if err != nil {
		return
	}

}
