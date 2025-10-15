package mock

import (
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/internal/server"
)

func StartJoinerMock(configPath string) {
	conf, err := config.InitConfig(configPath)
	if err != nil {
		panic(err)
	}
	server := server.InitServer(conf)
	server.Run()
}
