package main

import (
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/internal/server"
	"github.com/maxogod/distro-tp/src/common/logger"
)

func main() {

	conf, _ := config.InitConfig()

	logger.InitLogger(logger.LoggerEnvironment(conf.LogLevel))

	logger.Logger.Infof(conf.String())

	server := server.InitServer(conf)

	server.Run()
	logger.Logger.Infof("Dugtrio thanks you for using the Aggregator Worker!")
}
