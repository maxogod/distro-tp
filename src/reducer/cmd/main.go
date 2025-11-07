package main

import (
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/reducer/config"
	"github.com/maxogod/distro-tp/src/reducer/internal/server"
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
	logger.Logger.Infof("Dratini thanks you for using the Reducer Worker!")

}
