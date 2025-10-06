package main

import (
	"os"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/internal/server"
)

var log = logger.GetLogger()

func main() {
	time.Sleep(12 * time.Second)

	conf, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	log.Debug(conf.String())

	if err = os.MkdirAll(conf.StorePath, 0755); err != nil {
		log.Errorf("failed to create output directory: %v", err)
		return
	}

	server := server.InitServer(conf)

	server.Run()
	log.Debug("Charmander thanks you for using the Joiner!")

}
