package main

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/controller/config"
	"github.com/maxogod/distro-tp/src/controller/internal/server"
)

func main() {
	conf, _ := config.InitConfig()

	logger.InitLogger(logger.LoggerEnvironment(conf.LogLevel))

	logger.Logger.Infof("Controller server starting")
	server := server.NewServer(conf)
	err := server.Run()
	if err != nil {
		logger.Logger.Fatalf("Failed to run server: %v", err)
	}

	logger.Logger.Infof("Creselia finished successfully!")
}
