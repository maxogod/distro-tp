package main

import (
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/filter/config"
	"github.com/maxogod/distro-tp/src/filter/internal/server"
)

var log = logger.GetLogger()

func main() {
	time.Sleep(12 * time.Second)

	conf, err := config.InitConfig()
	if err != nil {
		log.Fatalf("%s", err)
	}

	log.Debug(conf.String())

	server := server.InitServer(conf)

	server.Run()
	log.Debug("Squirtle thanks you for using the Filter Worker!")

}
