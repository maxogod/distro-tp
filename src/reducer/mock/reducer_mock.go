package mock

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/reducer/config"
	"github.com/maxogod/distro-tp/src/reducer/internal/server"
)

func StartReducerMock(configPath string) {
	conf, _ := config.InitConfig(configPath)
	logger.InitLogger(logger.LoggerEnvDevelopment)
	server := server.InitServer(conf)
	server.Run()
}
