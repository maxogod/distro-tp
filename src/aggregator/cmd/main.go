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
	time.Sleep(12 * time.Second)

	conf, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	if err = os.MkdirAll(conf.StorePath, 0755); err != nil {
		log.Errorf("failed to create output directory: %v", err)
		return
	}

	log.Debug(conf.String())

	server := server.InitServer(conf)

	server.Run()
	log.Debug("Squirtle thanks you for using the Filter Worker!")

}
