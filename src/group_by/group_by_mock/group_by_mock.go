package group_by_mock

import (
	"github.com/maxogod/distro-tp/src/group_by/config"
	"github.com/maxogod/distro-tp/src/group_by/internal/server"
)

func StartGroupByMock(configPath string) {
	conf, err := config.InitConfig(configPath)
	if err != nil {
		panic(err)
	}
	server := server.InitServer(conf)
	server.Run()
}
