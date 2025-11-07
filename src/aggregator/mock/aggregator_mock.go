package mock

import (
	"github.com/maxogod/distro-tp/src/aggregator/config"
	"github.com/maxogod/distro-tp/src/aggregator/internal/server"
	"github.com/maxogod/distro-tp/src/common/logger"
)

func StartAggregatorMock(configPath string) {
	conf, _ := config.InitConfig(configPath)
	logger.InitLogger(logger.LoggerEnvDevelopment)
	server := server.InitServer(conf)
	server.Run()
}
