package main

import (
	"os"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/filter/config"
	"github.com/maxogod/distro-tp/src/filter/internal/server"
)

var log = logger.GetLogger()

func main() {

	var configPath string
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	time.Sleep(12 * time.Second)

	conf, err := config.InitConfig(configPath)
	if err != nil {
		log.Fatalf("%s", err)
	}

	log.Debug(conf.String())

	server := server.InitServer(conf)

	server.Run()
	log.Debug("Squirtle thanks you for using the Filter Worker!")

}
