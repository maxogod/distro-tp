package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type HeartbeatConfig struct {
	Host     string
	Port     int
	Interval int
}

type Config struct {
	Address   string
	LogLevel  string
	Heartbeat HeartbeatConfig
}

func (c Config) String() string {
	return fmt.Sprintf(
		"Address: %s | LogLevel: %s",
		c.Address,
		c.LogLevel,
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

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Interval: v.GetInt("heartbeat.interval"),
	}

	config := &Config{
		Address:   v.GetString("gateway.address"),
		LogLevel:  v.GetString("log.level"),
		Heartbeat: heatbeatConf,
	}

	return config, nil
}
