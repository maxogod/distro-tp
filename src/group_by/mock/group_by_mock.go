package mock

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/group_by/config"
	"github.com/maxogod/distro-tp/src/group_by/internal/server"
)

func StartGroupByMock(configPath string) {
	conf, _ := config.InitConfig(configPath)
	logger.InitLogger(logger.LoggerEnvDevelopment)
	server := server.InitServer(conf)
	server.Run()
}
