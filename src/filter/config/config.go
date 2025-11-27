package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/maxogod/distro-tp/src/filter/internal/task_executor"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type HeartbeatConfig struct {
	Host     string
	Port     int
	Interval time.Duration
}

type Config struct {
	Address    string
	LogLevel   string
	TaskConfig task_executor.TaskConfig
	Heartbeat  HeartbeatConfig
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

	heatbeatConf := HeartbeatConfig{
		Host:     v.GetString("heartbeat.host"),
		Port:     v.GetInt("heartbeat.port"),
		Interval: time.Duration(v.GetInt("heartbeat.interval")) * time.Second,
	}

	config := &Config{
		Address:  v.GetString("gateway.address"),
		LogLevel: v.GetString("log.level"),
		TaskConfig: task_executor.TaskConfig{
			FilterYearFrom:       v.GetInt("filter.year.from"),
			FilterYearTo:         v.GetInt("filter.year.to"),
			BusinessHourFrom:     v.GetInt("filter.businessHours.from"),
			BusinessHourTo:       v.GetInt("filter.businessHours.to"),
			TotalAmountThreshold: v.GetFloat64("filter.totalAmountThreshold"),
		},
		Heartbeat: heatbeatConf,
	}

	return config, nil
}
