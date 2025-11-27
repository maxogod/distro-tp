package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type HeartbeatConfig struct {
	Host     string
	Port     int
	Interval time.Duration
}

type Config struct {
	Address              string
	LogLevel             string
	AmountOfUsersPerFile int
	Heartbeat            HeartbeatConfig
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

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Interval: time.Duration(v.GetInt("heartbeat.interval")) * time.Second,
	}

	config := &Config{
		Address:              v.GetString("gateway.address"),
		LogLevel:             v.GetString("log.level"),
		AmountOfUsersPerFile: v.GetInt("amount_of_users_per_file"),
		Heartbeat:            heatbeatConf,
	}

	return config, nil
}
