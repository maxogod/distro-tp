package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	GatewayAddress string
	StorePath      string
	LogLevel       string
	StoreTPVQueue  string
}

func (c Config) String() string {
	return fmt.Sprintf(
		"GatewayAddress: %s | LogLevel: %s | StorePath: %s | StoreTPVQueue: %s",
		c.GatewayAddress,
		c.StorePath,
		c.LogLevel,
		c.StoreTPVQueue,
	)
}

const CONFIG_FILE_PATH = "/config.yaml"

func InitConfig() (*Config, error) {

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetConfigFile(CONFIG_FILE_PATH)
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "failed to read config file %s", CONFIG_FILE_PATH)
	}

	config := &Config{
		GatewayAddress: v.GetString("gateway.address"),
		StorePath:      v.GetString("datasets.path"),
		LogLevel:       v.GetString("log.level"),
		StoreTPVQueue:  v.GetString("store.tpv_queue"),
	}

	return config, nil
}
