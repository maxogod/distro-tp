package main

import (
	"os"
	"sync"
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

	var wg sync.WaitGroup
	for _, t := range os.Args[1:] {
		c, err := client.NewClient(conf)
		if err != nil {
			logger.Logger.Debugf("failed to create client: %v", err)
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err = c.Start(t); err != nil {
				logger.Logger.Debugf("Client error: %v", err)
			}
		}()
	}

	wg.Wait()

	after := time.Now()

	logger.Logger.Infof("Pikachu finished successfully in %s\n", after.Sub(before).String())
}
