package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	ServerHost string
	ServerPort int
	DataPath   string
}

func (c *Config) String() string {
	return fmt.Sprintf("ServerHost: %s, ServerPort: %d, DataPath: %s", c.ServerHost, c.ServerPort, c.DataPath)
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
		DataPath:   v.GetString("data_path"),
	}

	fmt.Printf("Config loaded: %s\n", config.String())

	return config, nil
}
