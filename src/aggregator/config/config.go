package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	Address   string
	LogLevel  string
	StorePath string
	BatchSize int
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Address: %s | LogLevel: %s | storePath: %s | BatchSize: %d",
		c.Address,
		c.LogLevel,
		c.StorePath,
		c.BatchSize,
	)
}

const CONFIG_FILE_PATH = "./config.yaml"

func InitConfig() (*Config, error) {

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetConfigFile(CONFIG_FILE_PATH)
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "failed to read config file %s", CONFIG_FILE_PATH)
	}

	config := &Config{
		Address:   v.GetString("gateway.address"),
		LogLevel:  v.GetString("log.level"),
		StorePath: v.GetString("datasets.path"),
		BatchSize: v.GetInt("datasets.batch_size"),
	}

	return config, nil
}
