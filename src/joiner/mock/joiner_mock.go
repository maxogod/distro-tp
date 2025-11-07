package mock

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/joiner/config"
	"github.com/maxogod/distro-tp/src/joiner/internal/server"
)

func StartJoinerMock(configPath string) {
	conf, _ := config.InitConfig(configPath)
	logger.InitLogger(logger.LoggerEnvDevelopment)
	server := server.InitServer(conf)
	server.Run()
}
