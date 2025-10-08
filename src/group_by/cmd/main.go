package main

import (
	"os"

	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/group_by/config"
	"github.com/maxogod/distro-tp/src/group_by/internal/server"
)

var log = logger.GetLogger()

func main() {

	var configPath string
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	conf, err := config.InitConfig(configPath)
	if err != nil {
		log.Fatalf("%s", err)
	}

	log.Debug(conf.String())

	server := server.InitServer(conf)

	server.Run()
	log.Debug("Togepi thanks you for using the Group_By Worker!")

}
