package mock

import (
	"github.com/maxogod/distro-tp/src/reducer/config"
	"github.com/maxogod/distro-tp/src/reducer/internal/server"
)

func StartReducerMock(configPath string) {
	conf, err := config.InitConfig(configPath)
	if err != nil {
		panic(err)
	}
	server := server.InitServer(conf)
	server.Run()
}
