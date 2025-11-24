package main

import (
	"os"
	"time"

	"github.com/maxogod/distro-tp/src/client/config"
	"github.com/maxogod/distro-tp/src/client/internal/client"
	"github.com/maxogod/distro-tp/src/common/logger"
)

func main() {
	before := time.Now()

	conf, _ := config.InitConfig()

	logger.InitLogger(logger.LoggerEnvironment(conf.LogLevel))

	if len(os.Args) < 2 {
		logger.Logger.Fatalln("no arguments provided")
	}

	logger.Logger.Infof("Client will do tasks: %v", os.Args[1:])
	for _, t := range os.Args[1:] {
		c, err := client.NewClient(conf)
		if err != nil {
			logger.Logger.Fatalf("failed to create client: %v", err)
		}

		if err := c.Start(t); err != nil {
			logger.Logger.Fatalln("Client error:", err)
		}
	}

	after := time.Now()

	logger.Logger.Infof("Pikachu finished successfully in %s\n", after.Sub(before).String())
}
