package mock

import (
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/internal/server"
)

func StartAggregatorMock(configPath string) {
	conf, err := config.InitConfig(configPath)
	if err != nil {
		panic(err)
	}
	server := server.InitServer(conf)
	server.Run()
}
