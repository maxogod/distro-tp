package main

import (
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/group_by/config"
	"github.com/maxogod/distro-tp/src/group_by/internal/server"
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
	logger.Logger.Infof("Togepi thanks you for using the Group_By Worker!")

}
