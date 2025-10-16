package mock

import (
	"github.com/maxogod/distro-tp/src/filter/config"
	"github.com/maxogod/distro-tp/src/filter/internal/server"
)

func StartFilterMock(configPath string) {
	conf, err := config.InitConfig(configPath)
	if err != nil {
		panic(err)
	}
	server := server.InitServer(conf)
	server.Run()
}
