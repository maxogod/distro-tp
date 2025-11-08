package main

import (
	"os"

	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/internal/server"
	"github.com/maxogod/distro-tp/src/common/logger"
)

func main() {
	var configPath string
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	conf, _ := config.InitConfig(configPath)

	logger.InitLogger(logger.LoggerEnvironment(conf.LogLevel))

	logger.Logger.Infof(conf.String())

	server := server.InitServer(conf)

	server.Run()
	logger.Logger.Infof("Dugtrio thanks you for using the Aggregator Worker!")
}
