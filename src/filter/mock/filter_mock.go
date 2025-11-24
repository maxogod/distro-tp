package mock

import (
	"github.com/maxogod/distro-tp/src/common/logger"
	"github.com/maxogod/distro-tp/src/filter/config"
	"github.com/maxogod/distro-tp/src/filter/internal/server"
)

func StartFilterMock(configPath string) {
	conf, _ := config.InitConfig(configPath)
	logger.InitLogger(logger.LoggerEnvDevelopment)
	server := server.InitServer(conf)
	server.Run()
}
