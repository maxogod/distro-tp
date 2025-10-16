package main

import (
	"os"
	"time"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/gateway/config"
	"github.com/maxogod/distro-tp/src/gateway/internal/client"
)

var log = logger.GetLogger()

func main() {
	before := time.Now()

	if len(os.Args) < 2 {
		log.Fatalln("no arguments provided")
	}

	conf, err := config.InitConfig()
	if err != nil {
		log.Fatalln("failed to initialize config:", err)
	}

	log.Debugf("Client will do tasks: %v", os.Args[1:])
	for _, t := range os.Args[1:] {
		c, err := client.NewClient(conf)
		if err != nil {
			log.Fatalf("failed to create client: %v", err)
		}

		if err := c.Start(t); err != nil {
			log.Fatalln("Client error:", err)
		}
	}

	after := time.Now()

	log.Debugf("Pikachu finished successfully in %s\n", after.Sub(before).String())
}
