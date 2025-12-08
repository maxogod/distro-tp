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
	Amount   int
	Interval time.Duration
}

type Config struct {
	ID        string
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

	v.BindEnv("id", "ID")
	v.BindEnv("heartbeat.amount", "EOL_NODES")

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Amount:   v.GetInt("heartbeat.amount"),
		Interval: time.Duration(v.GetInt("heartbeat.interval")) * time.Millisecond,
	}

	config := &Config{
		ID:        v.GetString("id"),
		Address:   v.GetString("gateway.address"),
		LogLevel:  v.GetString("log.level"),
		Heartbeat: heatbeatConf,
	}

	return config, nil
}
