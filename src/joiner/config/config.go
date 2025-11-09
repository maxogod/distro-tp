package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	Address              string
	LogLevel             string
	AmountOfUsersPerFile int
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Address: %s | LogLevel: %s | AmountOfUsersPerFile: %d",
		c.Address,
		c.LogLevel,
		c.AmountOfUsersPerFile,
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
		Address:              v.GetString("gateway.address"),
		LogLevel:             v.GetString("log.level"),
		AmountOfUsersPerFile: v.GetInt("amount_of_users_per_file"),
	}

	return config, nil
}
