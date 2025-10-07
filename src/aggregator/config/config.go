package config

import (
	"fmt"
	"strings"

	"github.com/maxogod/distro-tp/src/aggregator/internal/task_executor"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	Address    string
	LogLevel   string
	TaskConfig task_executor.TaskConfig
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Address: %s | LogLevel: %s | TaskConfig: [%s]",
		c.Address,
		c.LogLevel,
		c.TaskConfig.String(),
	)
}

const CONFIG_FILE_PATH = "./config.yaml"

func InitConfig(configFilePath string) (*Config, error) {

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	configFile := CONFIG_FILE_PATH
	if configFilePath != "" {
		configFile = configFilePath
	}

	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "failed to read config file %s", configFile)
	}

	config := &Config{
		Address:  v.GetString("gateway.address"),
		LogLevel: v.GetString("log.level"),
		TaskConfig: task_executor.TaskConfig{
			Persistence: v.GetBool("persistence.enabled"),
		},
	}

	return config, nil
}
