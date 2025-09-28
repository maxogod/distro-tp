package config

import (
	"errors"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	ServerHost string
	ServerPort int
}

const CONFIG_FILE_PATH = "./config.yaml"

func InitConfig() (*Config, error) {

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetConfigFile(CONFIG_FILE_PATH)
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.New("failed to read config file " + CONFIG_FILE_PATH)
	}

	config := &Config{
		ServerHost: v.GetString("server.host"),
		ServerPort: v.GetInt("server.port"),
	}

	return config, nil
}
